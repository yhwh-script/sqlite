
import { log, error } from './Logger';

if (window.Worker) {
} else {
    error('Your browser doesn\'t support web workers.');
}

const worker = new Worker("./modules/worker/SQLiteWorker.js", { type: 'module' });

let returnValues = {}; // worker returned values go here by timestamp

worker.onmessage = function (event) {
    const data = event.data;
    switch (data.type) {
        case "application/x-sqlite3": { // db download ready 
            let downloadChannel = new BroadcastChannel("download_channel");
            downloadChannel.postMessage(data);
            downloadChannel.close();
            break;
        }
        case "application/json":
            returnValues[data.timestamp] = structuredClone(data.result);
            break;
        default:
            log("response from worker:", data);
    }
}

function getWorker() {
    return new Promise((resolve) => {
        const checkAgain = function () {
            if (worker) {
                resolve(worker);
            } else
                setTimeout(checkAgain, 0)
        }
        checkAgain();
    });
}

export async function executeQuery({ text, values }) {
    let queryString = text;
    if (values && queryString.indexOf("$") != -1) values.forEach(function replacePlaceholder(item, index) { queryString = queryString.replace("$" + (index + 1), `'${item}'`); });
    const worker = await getWorker();
    let timestamp = Date.now();
    worker.postMessage({ timestamp, type: "exec", sql: queryString, returnValue: "resultRows" });
    try {
        return new Promise((resolve) => {
            const checkAgain = function () {
                if (returnValues[timestamp]) {
                    const returnValue = structuredClone(returnValues[timestamp]);
                    resolve(returnValue);
                } else
                    setTimeout(checkAgain, 0);
            }
            checkAgain();
        });
    } finally {
        delete returnValues[timestamp];
    }
}

export async function executeQuerySync({ text, values }) {
    let queryString = text;
    if (values && queryString.indexOf("$") === -1) values.forEach(function replacePlaceholder(item, index) { queryString = queryString.replace("$" + (index + 1), `'${item}'`); });
    const message = { type: "exec", sql: queryString, returnValue: "resultRows" };
    const worker = await getWorker();
    worker.postMessage(JSON.stringify(message));
}

export function uploadSync(arrayBuffer) {
    worker.postMessage({ "type": "upload", "buffer": arrayBuffer });
}

export function downloadSync() {
    worker.postMessage({ "type": "download" });
}
