#!node
const fs = require('fs').promises;

async function main() {
  const letters = process.argv[2];
  const filePath = process.argv[3];


  const fileContent = (await fs.readFile(filePath)).toString();
  const sentences = fileContent.match(/[^.!?]+[.!?]/g) || [];

  const promises = [];
  for (const letter of letters.split(',')) {
    const sentencesWithFindLetter = sentences.filter(sentence => new RegExp(`\\b${letter}\\b`).test(sentence)).map(s => s.trim());

    const path = `/golem/work/sentences_with_letter_${letter}`;
    promises.push(fs.writeFile(
      path,
      sentencesWithFindLetter.join('\n')
    ));

    process.stdout.write(path + "\n");
  }

  await Promise.all(promises);
}


main();
