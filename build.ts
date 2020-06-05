import _ from 'lodash';
import request from 'request';
import https from 'https';
import csv from 'csv';
import fs from 'fs';
import path from 'path';
import moment from 'moment';

const startTime = _.now();

let latestDate: Date | null = null;
let date90DaysAgo: Date | null = null;

const stateNameToFips: Record<string, string> = {};

type CsvRowObj = Record<string, string | number | null | undefined>;
type RecordHandler = (row: CsvRowObj) => void;

function parseNytCsv(url: string, onRecord: RecordHandler) {
  return new Promise((resolve, reject) => {
    // @ts-ignore
    const countiesParser = csv.parse({cast: false, cast_date: false, columns: true});

    https.get(url, response => {
      response.pipe(countiesParser);
    });

    countiesParser.on('data', function(record: CsvRowObj) {
      onRecord(record);
    });
    countiesParser.on('end', function() {
      resolve();
    });
    countiesParser.on('error', function(error: Error) {
      reject(error);
    });
  });
}

function parseCTJson(url: string): Promise<any[]> {
  return new Promise((resolve, reject) => {
    request(
      {
        url: url,
        json: true,
      },
      function(error, response, body) {
        if (!error && response.statusCode === 200) {
          resolve(body);
        } else {
          reject(error);
        }
      },
    );
  });
}

async function parseNytUs() {
  const usRows: CsvRowObj[] = [];

  await parseNytCsv(
    'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv',
    (record: CsvRowObj) => {
      const date = record.date as string;
      const d = createDate(date);
      if (!latestDate || d.getTime() > latestDate.getTime()) {
        latestDate = d;
      }
      usRows.push(record);
    },
  );

  date90DaysAgo = moment(latestDate)
    // Need extra 7 days of data for 7-day moving average
    .subtract(97, 'days')
    .toDate();

  return usRows;
}

async function parseNytStates() {
  const rows: CsvRowObj[] = [];

  await parseNytCsv(
    'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-states.csv',
    (record: CsvRowObj) => {
      const state = record.state as string;
      const fips: string | number = record.fips as any;

      // Populate our state name to fips mapping (used for csv files as well)
      if (!stateNameToFips[state]) {
        stateNameToFips[state] = String(fips);
      }

      rows.push(record);
    },
  );

  return rows;
}

async function parseNytCounties(): Promise<Record<string, CsvRowObj[]>> {
  const groupedByStateFips: Record<string, CsvRowObj[]> = {};

  await parseNytCsv(
    'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv',
    (record: CsvRowObj) => {
      const stateFips = stateNameToFips[record.state as string];
      if (!groupedByStateFips[stateFips]) {
        groupedByStateFips[stateFips] = [];
      }
      groupedByStateFips[stateFips].push(record);
    },
  );

  return groupedByStateFips;
}

async function parseCtUs() {
  return parseCTJson('https://covidtracking.com/api/v1/us/daily.json');
}

async function parseCtStates() {
  return parseCTJson('https://covidtracking.com/api/v1/states/daily.json');
}

async function buildStateFiles() {
  // Parse US data first
  const [nytUsData, ctUsData] = await Promise.all([parseNytUs(), parseCtUs()]);

  // Add fips: '00' to ctUsData to keep matching code consistent
  ctUsData.forEach((row: any) => {
    row.fips = '00';
  });

  // Then parse state-level data
  const [nytStateData, ctStateData] = await Promise.all([parseNytStates(), parseCtStates()]);

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
  const nytData = [
    ...nytUsData.map(({date, cases, deaths}) => ({date, state: 'US', fips: '00', cases, deaths})),
    ...nytStateData,
  ];
  const ctData = [...ctUsData, ...ctStateData];

  processData(nytData, row => row.state, ctData);

  const columns = [
    'date',
    'state',
    'fips',
    'cases',
    'newCases',
    'deaths',
    'newDeaths',
    'tests',
    'newTests',
    'positive',
    'newPositive',
    'negative',
    'newNegative',
    'pending',
  ];

  // Write "all" file
  await writeCsvFile(nytData, columns, './data/nyt/state/all.csv');

  // Write "90d" file
  const rows90days = nytData.filter((row: any) => {
    return createDate(row.date as string).getTime() >= (date90DaysAgo as Date).getTime();
  });
  await writeCsvFile(rows90days, columns, './data/nyt/state/90d.csv');
}

async function buildCountyFiles() {
  const groupedByStateFips = await parseNytCounties();

  // Clear output directories
  await clearDir(path.resolve(__dirname, './data/nyt/county/90d'));
  await clearDir(path.resolve(__dirname, './data/nyt/county/all'));

  const columns = ['date', 'county', 'state', 'fips', 'cases', 'newCases', 'deaths', 'newDeaths'];

  for (const fips in groupedByStateFips) {
    const rows = groupedByStateFips[fips];
    const safeFips = fips.replace(/\D/g, '');

    processData(rows, row => `${row.state}^${row.county}`);

    // Write "all" file
    await writeCsvFile(rows, columns, `./data/nyt/county/all/${safeFips}.csv`);

    // Write "90d" file
    const rows90days = rows.filter(row => {
      return createDate(row.date as string).getTime() >= (date90DaysAgo as Date).getTime();
    });
    await writeCsvFile(rows90days, columns, `./data/nyt/county/90d/${safeFips}.csv`);
  }
}

function writeCsvFile(rows: any[], columns: string[], filePath: string) {
  return new Promise((resolve, reject) => {
    // @ts-ignore
    csv.stringify(rows, {header: true, columns}, (err: Error | null, output: string) => {
      if (err) {
        reject(err);
        return;
      }
      fs.writeFileSync(path.resolve(__dirname, filePath), output);
      resolve();
    });
  });
}

function processData(data: any[], groupBy: (row: any) => string, testingData?: any[]) {
  if (testingData) {
    // Need to group testing data by (1) fips, (2) date
    const groupedByFips: Record<string, Record<string, CsvRowObj>> = _(testingData)
      .groupBy(row => row.fips)
      .mapValues(rows => {
        return _.keyBy(rows, row => {
          const ds = String(row.date);
          return `${ds.substring(0, 4)}-${ds.substring(4, 6)}-${ds.substring(6, 8)}`;
        });
      })
      .value();

    data.forEach(row => {
      const dataForFips = groupedByFips[row.fips] || ({} as any);
      const dataOnDate = dataForFips[row.date];
      if (dataOnDate) {
        row.positive = coerceNumber(dataOnDate.positive);
        row.negative = coerceNumber(dataOnDate.negative);
        row.pending = coerceNumber(dataOnDate.pending);
        row.tests = coerceNumber(dataOnDate.total);
        row.newPositive = coerceNumber(dataOnDate.positiveIncrease);
        row.newNegative = coerceNumber(dataOnDate.negativeIncrease);
        row.newTests = coerceNumber(dataOnDate.totalTestResultsIncrease);
      }
    });
  }

  // Calculate newCases/newDeaths properties
  const grouped = _.groupBy(data, groupBy);
  for (const group in grouped) {
    const groupRows = grouped[group];
    let lastCases = Number(groupRows[0].cases);
    let lastDeaths = Number(groupRows[0].deaths);

    // Initialize first row
    groupRows[0].newCases = lastCases;
    groupRows[0].newDeaths = lastDeaths;

    // Handle subsequent rows
    for (let i = 1; i < groupRows.length; i++) {
      const row = groupRows[i];
      row.newCases = Number(row.cases) - lastCases;
      row.newDeaths = Number(row.deaths) - lastDeaths;
      lastCases = Number(row.cases);
      lastDeaths = Number(row.deaths);
    }
  }
}

function coerceNumber(value: unknown) {
  return value == undefined ? null : Number(value);
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
  await buildStateFiles();
  await buildCountyFiles();
}

run();
