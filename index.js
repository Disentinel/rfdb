// Simple loader for development - just load the local .node file
const nativeBinding = require('./navi-graph-engine.node')

const { GraphEngine, computeNodeIdJs, computeNodeIdFromString } = nativeBinding

module.exports.GraphEngine = GraphEngine
module.exports.computeNodeId = computeNodeIdJs
module.exports.computeNodeIdFromString = computeNodeIdFromString

// Some JS code expects computeNodeIdJs to be an instance method on GraphEngine
// Attach the native function to the prototype so calls like
//    this.engine.computeNodeIdJs(...)
// will work when `GraphEngine` is instantiated in JS.
if (GraphEngine && typeof computeNodeIdJs === 'function') {
	GraphEngine.prototype.computeNodeIdJs = computeNodeIdJs;
}
