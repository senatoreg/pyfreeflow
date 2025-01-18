from .types import FreeFlowExt
import jwt
import hashlib
import logging
import datetime as dt
from ..utils import DurationParser

__TYPENAME__ = "JwtOperator"


"""
run parameter:
{
  "state": { ... },
  "data": {
    "op": "encode|decode",  # operation (default encode)
    "headers": {},  # for encode (optional)
    "algorithm": {},  # for encode (optional)
    "body": ""  # for encode
    "token": ""  # for decode
    "headers_only": True|False  # only unverified headers (default False)
  }
}
"""


class JwtOperatorV1_0(FreeFlowExt):
    __typename__ = __TYPENAME__
    __version__ = "1.0"

    def __init__(self, name, pubkey_files, privkey_files, algorithms=["HS256"],
                 headers={}, verify_sign=True, verify_exp=True,
                 required_claims=[], duration=None, not_before=None,
                 issuer=None, max_tasks=4):
        super().__init__(name, max_tasks=max_tasks)

        self._algorithms = algorithms

        self._pub_key = {}
        for key_file in pubkey_files:
            with open(key_file, "rb") as f:
                content = f.read()
                h = hashlib.sha256(content).hexdigest()
                self._pub_key[h] = content

        self._priv_key = {}
        for key_file in privkey_files:
            with open(key_file, "rb") as f:
                content = f.read()
                h = hashlib.sha256(content).hexdigest()
                self._priv_key[h] = content

        self._default_pub_key = list(self._pub_key.keys())[0]
        self._default_priv_key = list(self._priv_key.keys())[0]

        self._headers = headers
        self._options = {
            "require": [x for x in required_claims],
            "verify_signature": verify_sign,
            "verify_exp": verify_exp,
        }

        self._duration = DurationParser.parse(duration) if isinstance(duration, str) else None
        self._not_before = DurationParser.parse(not_before) if isinstance(not_before, str) else None
        self._issuer = issuer

        self._logger = logging.getLogger(".".join([__name__, self.__typename__,
                                                   self._name]))

        self._action = {
            "encode": self._do_encode,
            "decode": self._do_decode,
        }

    def _get_priv_key(self, data):
        kid = data.get("kid", self._default_priv_key)
        return kid, self._priv_key[kid]

    def _get_pub_key(self, headers):
        kid = headers.get("kid", self._default_pub_key)
        return kid, self._pub_key[kid]

    async def _do_encode(self, data):
        body = data.get("body")

        if not isinstance(body, dict):
            self._logger.error("Invalid input format '{}'".format(type(body)))
            return None, 101

        hdr = self._headers | data.get("headers", {})

        kid, key = self._get_priv_key(data)

        hdr["kid"] = kid

        if self._duration is not None and "exp" not in body:
            body["exp"] = int(dt.datetime.now(dt.timezone.utc).timestamp() + self._duration)

        if self._not_before is not None and "nbf" not in body:
            body["nbf"] = int(dt.datetime.now(dt.timezone.utc).timestamp() + self._not_before)

        if "iat" not in body:
            body["iat"] = int(dt.datetime.now(dt.timezone.utc).timestamp())

        if self._issuer is not None and "iss" not in body:
            body["iss"] = self._issuer

        algorithm = data.get("algorithm", self._algorithms[0])

        return {"token": jwt.encode(body, key,
                                    algorithm=algorithm, headers=hdr)}, 0

    async def _do_decode(self, data):
        token = data.get("token")
        hdr_only = data.get("headers_only", False)

        if not isinstance(token, str):
            self.logger.error("Invalid input format '{}'".format(type(token)))
            return None, 101

        try:
            hdr = jwt.get_unverified_header(token)

            kid, key = self._get_pub_key(hdr)

            if not hdr_only:
                body = jwt.decode(token, key,
                                  algorithms=self._algorithms,
                                  options=self._options)
            else:
                body = None

            return {"headers": hdr, "body": body}, 0
        except Exception as ex:
            self._logger.error(ex)
            return {"headers": None, "body": None}, 102

    async def do(self, state, data):
        op = data.get("op", "encode")
        return state, await self._action[op](data)
