/* HyperFlow engine. 
 ** Converts from pegasus dax file to hyperflow workflow representation (json)
 ** Author: Bartosz Balis (2013-2018)
 */

/*
 * Any converter should provide an object constructor with the following API:
 * convert(wf, cb) 
 *   - @wf  - native workflow representation
 *   - @cb  - callback function (err, wfJson)
 * convertFromFile(filename, cb)
 *   - @filename  - file path from which the native wf representation should be read
 *   - @cb        - callback function (err, wfJson)
 */
var fs = require('fs'),
    xml2js = require('xml2js'),
    parse = require('shell-quote').parse;

// Pegasus DAX converter constructor, accepts optional name of function, used to execute tasks
var PegasusConverter = function(functionName) {
    if(typeof(functionName) === 'undefined') {
        this.functionName = "command_print";
    } else {
        this.functionName = functionName;
    }
}

PegasusConverter.prototype.convertFromFile = function(filename, cb) {
    var that = this;
    parseDax(filename, function(err, dax) {
        if (err) { 
            throw err; 
        } else {
            createWorkflow(dax, that.functionName, function(err, wfJson) {
                cb(null, wfJson);
            });
        }
    });
}


var sources = {}, sinks = {};

var nextTaskId = -1, nextDataId = -1, dataNames = {};

function parseDax(filename, cb) {
    var self = this;
    var parser = new xml2js.Parser({normalize: true});
    fs.readFile(filename, function(err, data) {
        if (err) {
            cb(new Error("File read error. Doesn't exist?"));
        } else {
            var dag = data.toString();
            // without the following replacements parsing of 'argument' elements is a hell 
            // (because DAX uses 'mixed content', i.e. elements mix text and child elements)
            dag = dag.replace(/<file name="(.*?)".*?\/>/g, "$1");  // DAX 3.x
            dag = dag.replace(/<filename file="(.*?)".*?\/>/g, "$1"); // DAX 2.1
            parser.parseString(dag, function(err, result) {
                if (err) {
                    cb(new Error("File parse error."));
                } else {
                    //console.log(JSON.stringify(result, null, 2));
                    cb(null, result);
                }
            });
        }
    });
}


function createWorkflow(dax, functionName, cb) {
    var wfOut = {
        //functions: [ {"name": functionName, "module": "functions"} ],
        processes: [],
        signals: [],
        ins: [],
        outs: []
    };

    // in DAX 3.x there can be "executable" elements
    var execs={};
    if (dax.adag.executable) {
        dax.adag.executable.forEach(function(exec) {
                var name = exec['$'].name,
                    execName = exec.pfn[0]['$'].url.replace(/^.*[\\\/]/, '');
                execs[name] = execName;
                //console.log(name, execName);
        });
    }

    dax.adag.job.forEach(function(job) {

        ++nextTaskId;
       
        var args = job.argument ? parse(job.argument[0]): [],
            jname = job['$'].name;

        wfOut.processes.push({
            "name": jname,
            "function": functionName,
            "type": "dataflow",
            "executor": "syscommand",
            "firingLimit": 1,
            "config": {
                "executor": {
                    "executable": execs[jname] ? execs[jname]: jname,
                    "args": args
                }
            },
            "ins": [],
            "outs": []
        });

        if (job.stdout) { // stdout should be redirected to a file
            wfOut.processes[nextTaskId].config.executor.stdout = job.stdout[0]['$'].name;
        }

        if (job['$'].runtime) { // synthetic workflow dax
            wfOut.processes[nextTaskId].runtime = job['$'].runtime;
        }

        var dataId, dataName;
        job.uses.forEach(function(job_data) {
            if (job_data['$'].name) {
                    dataName = job_data['$'].name; // dax v3.3
            } else {
                    dataName = job_data['$'].file; // dax v2.1
            }
            if (!dataNames[dataName]) {
                ++nextDataId;
                wfOut.signals.push({
                    "name": dataName,
                    "sources": [],
                    "sinks": []
                });
                dataId = nextDataId;
                dataNames[dataName] = dataId;
            } else {
                dataId = dataNames[dataName];
            }
            if (job_data['$'].size) { // synthetic workflow dax
                wfOut.signals[dataId].size = job_data['$'].size;
            }
            if (job_data['$'].link == 'input') {
                wfOut.processes[nextTaskId].ins.push(dataId);
                wfOut.signals[dataId].sinks.push(nextTaskId);
            } else {
                wfOut.processes[nextTaskId].outs.push(dataId);
                wfOut.signals[dataId].sources.push(nextTaskId);
            }
        });
    });

    for (var i=0; i<wfOut.signals.length; ++i) {
        if (wfOut.signals[i].sources.length == 0) {
            wfOut.ins.push(i);
            // the line below sends initial signals to the workflow. FIXME: add a cmd line option 
            wfOut.signals[i].data = [{ }]; 
        }
        if (wfOut.signals[i].sinks.length == 0) {
            wfOut.outs.push(i);
        }
    }

    for (var i=0; i<wfOut.signals.length; ++i) {
        if (wfOut.signals[i].sources.length > 1) {
            console.error("WARNING multiple sources for:" + wfOut.signals[i].name, "sinks:", wfOut.signals[i].sinks);
        }
        delete wfOut.signals[i].sources;
        delete wfOut.signals[i].sinks;
    }

    cb(null, wfOut);
}
                
function zeroPad(num, size) {
    var s = num+"";
    while (s.length < size) s = "0" + s;
    return s;
}

module.exports = PegasusConverter;
