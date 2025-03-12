import sqlite3InitModule from '@sqlite.org/sqlite-wasm';
import { log, info, error } from '../logger';

var db = null;
var sqlite3 = null;

onmessage = async function ({ data }) {
  const { action } = data;
  switch (action) {
    case 'createDB': {
      const { name } = data;
      const { newDB, message } = await createDatabase(name)
      db = newDB;
      postMessage({ type: 'created', message });
      break;
    }
    case 'executeQuery': {
      const { sql } = data;
      log(sql);
      try {
        const result = db.exec({ sql, returnValue: "resultRows" });
        postMessage({ result, type: "application/json" });
      } catch (e) {
        if (e.message.indexOf("SQLITE_CANTOPEN") != -1) {
          info("Info: Currently no SQLite database available for this worker. Upload a new database or reload the page.")
        }
      }
      break;
    }
    case 'uploadDB':
      const { name, arrayBuffer } = data;
      const { message } = await uploadDatabase(name, arrayBuffer)
      log(message, db);
      break;
    case 'downloadDB':
      try {
        const byteArray = sqlite3.capi.sqlite3_js_db_export(db);
        const blob = new Blob([byteArray.buffer], { type: "application/vnd.sqlite3" });
        postMessage(blob); // send the database Blob to the API
      } catch (e) {
        if (e.message.indexOf("SQLITE_NOMEM") != -1)
          postMessage({ type: "application/vnd.sqlite3", error: "SQLITE_NOMEM" });
        else
          error(e);
      }
      break;
    case 'closeDB':
      closeDB();
      postMessage({ type: "closed" });
      break;
    default:
      log(data)
  }
}

async function createDatabase(name) {
  const sqlite3 = await getInstance();
  return 'opfs' in sqlite3
    ? { newDB: new sqlite3.oo1.OpfsDb(`/${name}.sqlite3`), message: `OPFS is available, created persisted database at /${name}.sqlite3` }
    : { newDB: new sqlite3.oo1.DB(`/${name}.sqlite3`, 'ct'), message: `OPFS is not available, created transient database /${name}.sqlite3` };
}

async function uploadDatabase(name, arrayBuffer) {
  try {
    const sqlite3 = await getInstance();
    if ('opfs' in sqlite3) {
      const size = await sqlite3.oo1.OpfsDb.importDb(`${name}.sqlite3`, arrayBuffer);
      if (size) {
        db = new sqlite3.oo1.OpfsDb(`/${name}.sqlite3`);
        return { message: `New DB imported as ${name}.sqlite3. (${arrayBuffer.byteLength} Bytes)` }
      } else {
        throw new Error({ name: "ImportError", message: "Empty size" })
      }
    } else { // TODO allow alternative
      throw new Error({ name: "OPFSMissingError", message: "Unsupported operation due to missing OPFS support." });
    }
  } catch (err) {
    error(err.name, err.message);
  }
}

function closeDB() {
  if (db) {
    log("Closing...", db);
    db.close();
  }
}

async function getInstance() {
  try {
    if (!sqlite3) {
      sqlite3 = await sqlite3InitModule({ print: log, printErr: error });
    }
    return sqlite3;
  } catch (err) {
    error(err.name, err.message);
  }
}
