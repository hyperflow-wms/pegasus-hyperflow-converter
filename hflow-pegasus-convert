#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const PegasusConverter = require('./daxConverter');
const YamlConverter = require('./yamlConverter');

// Get command-line arguments (excluding "node" and the script path)
const args = process.argv.slice(2);

if (!args[0]) {
    console.log("Usage: hflow-pegasus-convert <workflow file path> [command_name]");
    console.log("   command_name can be: k8sCommand, redisCommand, amqpCommand, command_print or command... etc ");
    process.exit(1);
}

const filePath = args[0];
const ext = path.extname(filePath).toLowerCase();

let converter;
if (ext === '.yaml' || ext === '.yml') {
    converter = new YamlConverter();
} else {
    converter = args[1] ? new PegasusConverter(args[1]) : new PegasusConverter();
}

converter.convertFromFile(filePath, function (err, wfOut) {
    if (err) {
        console.error("Conversion error:", err);
        process.exit(1);
    }
    console.log(JSON.stringify(wfOut, null, 2));
});
