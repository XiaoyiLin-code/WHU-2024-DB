const express = require('express');
const bodyParser = require('body-parser');
const { exec } = require('child_process');
const path = require('path');

const app = express();
app.use(bodyParser.json());

app.post('/execute', (req, res) => {
    const command = req.body.command;
    const classPath = [
        path.join(__dirname, 'target/classes'),
        path.join(__dirname, 'lib/jsqlparser-4.6-SNAPSHOT.jar'),
        ...getJarFiles(path.join(__dirname, 'target/dependency'))
    ].join(path.delimiter);

    const javaProcess = exec(`java -cp "${classPath}" edu.whu.tmdb.Main`, { shell: true });

    let output = '';

    javaProcess.stdin.write(command + '\n');
    javaProcess.stdin.end();

    javaProcess.stdout.on('data', (data) => {
        output += data.toString();
    });

    javaProcess.stderr.on('data', (data) => {
        console.error(data.toString());
    });

    javaProcess.on('close', (code) => {
        if (code !== 0) {
            res.status(500).send(`Java process exited with code ${code}`);
        } else {
            res.send(output);
        }
    });
});

app.listen(3000, () => {
    console.log('Server is running on port 3000');
});

function getJarFiles(dir) {
    const fs = require('fs');
    return fs.readdirSync(dir).filter(file => file.endsWith('.jar')).map(file => path.join(dir, file));
}
