import fs from "fs";

/**
 * Decode PDF extract output — Python on Windows may emit UTF-16 LE.
 * @param {Buffer} buf
 */
export function decodeFddPlainTextBuffer(buf) {
  if (buf.length >= 2 && buf[0] === 0xff && buf[1] === 0xfe) {
    return buf.toString("utf16le");
  }
  return buf.toString("utf8");
}

/** @param {string} filePath */
export function readFddPlainTextFile(filePath) {
  return decodeFddPlainTextBuffer(fs.readFileSync(filePath));
}
