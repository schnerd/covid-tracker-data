import _ from 'lodash';
import https from 'https';
import csv from 'csv';
import fs from 'fs';
import path from 'path';
// const _ = require('lodash');
// const https = require('https');
// const csv = require('csv');
// const fs = require('fs');
// const path = require('path');

const startTime = _.now();

const stateNameToFips: Record<string, string> = {};
const expectedUsStatesHeader = ['date', 'state', 'fips', 'cases', 'deaths'];
const expectedUsCountiesHeader = ['date', 'county', 'state', 'fips', 'cases', 'deaths'];

type RecordHandler = (row: any[]) => void;

function parseNytCsv(url: string, onHeader: RecordHandler, onRecord: RecordHandler) {
  return new Promise((resolve, reject) => {
    // @ts-ignore
    const countiesParser = csv.parse({cast: false, cast_date: false});

    https.get(url, response => {
      response.pipe(countiesParser);
    });

    let isHeader = true;
    countiesParser.on('data', function(record: string[]) {
      if (isHeader) {
        onHeader(record);
        isHeader = false;
      } else {
        onRecord(record);
      }
    });
    countiesParser.on('end', function() {
      resolve();
    });
    countiesParser.on('error', function(error: Error) {
      reject(error);
    });
  });
}

async function parseStates() {
  function onHeader(record: string[]) {
    if (!_.isEqual(record, expectedUsStatesHeader)) {
      throw new Error(`Header for us-states.csv did not match, found: ['${record.join("', '")}']`);
    }
  }

  function onRecord(record: string[]) {
    const [date, state, fips, cases, deaths] = record;

    // Populate our state name to fips mapping (used for csv files as well)
    if (!stateNameToFips[state]) {
      stateNameToFips[state] = fips;
    }
  }

  await parseNytCsv(
    'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-states.csv',
    onHeader,
    onRecord,
  );
}

async function parseCounties() {
  const groupedByStateFips: Record<string, any[][]> = {};

  function onHeader(record: string[]) {
    if (!_.isEqual(record, expectedUsCountiesHeader)) {
      throw new Error(
        `Header for us-counties.csv did not match, found: ['${record.join("', '")}']`,
      );
    }
  }

  function onRecord(record: string[]) {
    const [, , state] = record;
    const stateFips = stateNameToFips[state];
    if (!groupedByStateFips[stateFips]) {
      groupedByStateFips[stateFips] = [];
    }
    groupedByStateFips[stateFips].push(record);
  }

  await parseNytCsv(
    'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv',
    onHeader,
    onRecord,
  );

  // Clear output directory
  await clearDir(path.resolve(__dirname, './data/nyt/county'));

  for (const fips in groupedByStateFips) {
    const rows = groupedByStateFips[fips];
    const safeFips = fips.replace(/\D/g, '');
    rows.unshift(expectedUsCountiesHeader);
    const result = rows.map(row => row.join(',')).join('\n');
    fs.writeFileSync(path.resolve(__dirname, `./data/nyt/county/${safeFips}.csv`), result);
  }
}

async function clearDir(directory: string) {
  return new Promise((resolve, reject) => {
    fs.readdir(directory, (err, files) => {
      if (err) {
        reject(err);
        return;
      }
      for (const file of files) {
        fs.unlinkSync(path.join(directory, file));
      }
      resolve();
    });
  });
}

async function run() {
  await parseStates();
  await parseCounties();
}

run();
