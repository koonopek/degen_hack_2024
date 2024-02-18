const { readFileSync } = require('node:fs');

function main() {
  const raport = {};

  for(const filePath of process.argv.slice(2)) {
    const data= readFileSync(filePath).toString().split('\n'); 

    let sum = 0;
    for(const value of data) {
      sum += Number(value.trim());
    }
    raport[filePath] = {
      count: data.length,
      positive: sum,
      negative: data.length - sum,
      percentagePositive: (sum / data.length).toFixed(3) 
    };
  }

  process.stdout.write(JSON.stringify(raport));
}

main();
