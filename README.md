# PyFreeFlow

## Definition

This library enables the creation of services by composing elementary modules into an operation flow.
It is designed to be usable even by non-programmers: the library automatically manages parallel execution and error handling. The user only needs to describe the process to be executed.

The flow is defined as a directed graph:

it can have 1..N input nodes and 1..N output nodes;

if multiple output nodes exist, one can be selected as the final result (all nodes are still executed);

each node’s output is passed as input to the next node in the graph.

A special module type, DataTransformer, allows data transformation between nodes with different types, formats, or structures. Since each node’s output is consumed by the following node, a state object is also passed to each node. This makes it possible to store data (usually through DataTransformer) for later use within the flow.

The library is extensible through dynamically loadable modules that follow a defined specification.

To configure a pipeline (the executable unit), two elements are required:

- the list of nodes;
- the graph definition, written in dot syntax.

## Command line tool

A command-line tool is included to execute pipelines defined in a Yaml file.
The file must contain three sections: ext, args, pipeline.

- **ext**: list of additional extensions to load.
- **args**: input parameters for the pipeline.
- **pipeline**: definition of the flow.

Example: password encryption

```yaml
ext:
- pyfreeflow.ext.crypto_operator
args:
  username: jdoe
  password: password
pipeline:
  digraph:
  - parseArgs -> createJson
  - createJson -> prepareEncrypt
  - prepareEncrypt -> encrypt
  - encrypt -> prepareSaveFile
  - prepareSaveFile -> saveFile
  name: crypto
  node:
  - config:
      transformer: |2
        data = {op = "write", data = {username = data.username, password = data.password}}
    name: parseArgs
    type: DataTransformer
    version: '1.0'
  - name: createJson
    type: JsonBufferOperator
    version: '1.0'
  - config:
      transformer: |2
        data = {op = "encrypt", data = data, key = "fernet.key"}
    name: prepareEncrypt
    type: DataTransformer
    version: '1.0'
  - name: encrypt
    type: FernetCryptoOperator
    version: '1.0'
  - config:
      transformer: |2
        data = {op = "write", data = data, path = "pass.enc"}
    name: prepareSaveFile
    type: DataTransformer
    version: '1.0'
  - config:
      binary: false
    name: saveFile
    type: AnyFileOperator
    version: '1.0'
```

Pipeline configuration parameters

- **name**: pipeline name (optional)
- **digraph**: connections between nodes, identified by unique names
- **node**: node definitions used in the digraph

Node definition parameters

- **config**: configuration parameters specific to the module
- **name**: unique node name
- **type**: module type implementing the node
- **version**: version of the module

For details about individual modules, refer to their documentation.

# License

This software is available under dual licensing:

## Non-Commercial Use - AGPL v3
For non-commercial use, this software is released under the GNU Affero General Public License v3.0.
Any modifications must be shared under the same license.

## Commercial Use
For commercial use, please contact me.
