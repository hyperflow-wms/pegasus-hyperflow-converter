const fs = require('fs');
const yaml = require('js-yaml');

class YamlConverter {
  constructor() {}

  convertFromFile(filePath, callback) {
    try {
      const yamlContent = fs.readFileSync(filePath, 'utf8');
      const data = yaml.load(yamlContent);

      const signals = [];
      const signalIds = {};
      const processes = [];

      function getSignalId(filename) {
        if (!(filename in signalIds)) {
          signalIds[filename] = signals.length;
          signals.push(filename);
        }
        return signalIds[filename];
      }

      function extractIoFiles(usesList, type) {
        return usesList.filter(entry => entry.type === type).map(entry => entry.lfn);
      }

      const allInputFiles = new Set();
      const allOutputFiles = new Set();

      data.jobs.forEach(job => {
        const ins = extractIoFiles(job.uses, "input").map(getSignalId);
        const outs = extractIoFiles(job.uses, "output").map(getSignalId);

        extractIoFiles(job.uses, "input").forEach(file => allInputFiles.add(file));
        extractIoFiles(job.uses, "output").forEach(file => allOutputFiles.add(file));

        processes.push({
          name: job.name,
          function: "{{function}}",
          type: "dataflow",
          firingLimit: 1,
          config: {
            executor: {
              executable: job.name,
              args: job.arguments || []
            }
          },
          ins: ins,
          outs: outs
        });
      });

      const inputSignalIndexes = [];
      const outputOnlySignalIndexes = [];

      const signalObjs = signals.map((name, index) => {
        const entry = { name };
        const isInput = allInputFiles.has(name);
        const isOutput = allOutputFiles.has(name);

        if (isInput && !isOutput) {
          entry.data = [{}];
          inputSignalIndexes.push(index);
        } else if (isOutput && !isInput) {
          outputOnlySignalIndexes.push(index);
        }

        return entry;
      });

      const result = {
        name: data.name || "workflow",
        processes: processes,
        signals: signalObjs,
        ins: inputSignalIndexes,
        outs: outputOnlySignalIndexes
      };

      callback(null, result);
    } catch (err) {
      callback(err);
    }
  }
}

module.exports = YamlConverter;
