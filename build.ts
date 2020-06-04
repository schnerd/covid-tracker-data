import _ from 'lodash';
import https from 'https';
import csv from 'csv';
import fs from 'fs';
import path from 'path';
import moment from 'moment';

const startTime = _.now();

let latestDate: Date | null = null;
let date90DaysAgo: Date | null = null;

const stateNameToFips: Record<string, string> = {};
const expectedUsHeader = ['date', 'cases', 'deaths'];
const expectedUsStatesHeader = ['date', 'state', 'fips', 'cases', 'deaths'];
const expectedUsCountiesHeader = ['date', 'county', 'state', 'fips', 'cases', 'deaths'];

const usRows: any[][] = [];

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

async function parseUs() {
  function onHeader(record: string[]) {
    if (!_.isEqual(record, expectedUsHeader)) {
      throw new Error(`Header for us.csv did not match, found: ['${record.join("', '")}']`);
    }
  }

  function onRecord(record: string[]) {
    const [date] = record;
    const d = createDate(date);
    if (!latestDate || d.getTime() > latestDate.getTime()) {
      latestDate = d;
    }
    usRows.push(record);
  }

  await parseNytCsv(
    'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv',
    onHeader,
    onRecord,
  );

  date90DaysAgo = moment(latestDate)
    // Need extra 7 days of data for 7-day moving average
    .subtract(97, 'days')
    .toDate();
}

async function parseStates() {
  function onHeader(record: string[]) {
    if (!_.isEqual(record, expectedUsStatesHeader)) {
      throw new Error(`Header for us-states.csv did not match, found: ['${record.join("', '")}']`);
    }
  }

  let rows: any[][] = [];

  function onRecord(record: string[]) {
    const [date, state, fips, cases, deaths] = record;

    // Populate our state name to fips mapping (used for csv files as well)
    if (!stateNameToFips[state]) {
      stateNameToFips[state] = fips;
    }

    rows.push(record);
  }

  await parseNytCsv(
    'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-states.csv',
    onHeader,
    onRecord,
  );

  // Remove existing files
  try {
    fs.unlinkSync(path.resolve(__dirname, './data/nyt/state/90d.csv'));
  } catch (err) {
    // Ignore missing file
  }
  try {
    fs.unlinkSync(path.resolve(__dirname, './data/nyt/state/all.csv'));
  } catch (err) {
    // Ignore missing file
  }

  // Prepend US data
  rows = [...usRows.map(([date, cases, deaths]) => [date, 'US', '00', cases, deaths]), ...rows];

  // Prepend header
  rows.unshift(expectedUsStatesHeader);

  // Write "all" file
  const result = rows.map(row => row.join(',')).join('\n');
  fs.writeFileSync(path.resolve(__dirname, `./data/nyt/state/all.csv`), result);

  // Write "90d" file
  const rows90days = rows.filter((row, i) => {
    return i === 0 || createDate(row[0]).getTime() >= (date90DaysAgo as Date).getTime();
  });
  const result90days = rows90days.map(row => row.join(',')).join('\n');
  fs.writeFileSync(path.resolve(__dirname, `./data/nyt/state/90d.csv`), result90days);
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

  // Clear output directories
  await clearDir(path.resolve(__dirname, './data/nyt/county/90d'));
  await clearDir(path.resolve(__dirname, './data/nyt/county/all'));

  for (const fips in groupedByStateFips) {
    const rows = groupedByStateFips[fips];
    const safeFips = fips.replace(/\D/g, '');

    rows.unshift(expectedUsCountiesHeader);

    // Write "all" file
    const result = rows.map(row => row.join(',')).join('\n');
    fs.writeFileSync(path.resolve(__dirname, `./data/nyt/county/all/${safeFips}.csv`), result);

    // Write "90d" file
    const rows90days = rows.filter((row, i) => {
      return i === 0 || createDate(row[0]).getTime() >= (date90DaysAgo as Date).getTime();
    });
    const result90days = rows90days.map(row => row.join(',')).join('\n');
    fs.writeFileSync(
      path.resolve(__dirname, `./data/nyt/county/90d/${safeFips}.csv`),
      result90days,
    );
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

function createDate(d: string): Date {
  return new Date(`${d} 00:00:00`);
}

async function run() {
  await parseUs();
  await parseStates();
  await parseCounties();
}

run();
