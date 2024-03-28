const express = require('express')
const axios = require('axios');
const cors = require('cors');
const app = express()
app.use(cors());
app.use(express.json());
const apiUrl = 'https://www.goethe.de/rest/examfinder/exams/institute/O%2010000611?category=E006&type=ER&countryIsoCode=&locationName=&count=24&start=1&langId=134&timezone=54&isODP=0&sortField=startDate&sortOrder=ASC&dataMode=0&langIsoCodes=de%2Cen%2Cvi';
let urls = [];
let isLog = true;
let status = '';

app.get('/api/get-btn', async (req, res) => {
  try {
    const cloneUrl = [...urls];
    const record = cloneUrl[Math.floor(Math.random() * cloneUrl.length)];
    res.json(record || null);
  } catch (error) {
    console.error('Lỗi truy vấn: ' + error.stack);
    res.status(200).json({ error: 'Lỗi truy vấn cơ sở dữ liệu' });
  }
});

app.get('/api/get-status', async (req, res) => {
  try {
    res.json(status);
  } catch (error) {
    console.error('Lỗi truy vấn: ' + error.stack);
    res.status(200).json({ error: 'Lỗi truy vấn cơ sở dữ liệu' });
  }
});

async function fetchDataFromApi() {
  try {
    const response = await axios.get(apiUrl, {
      timeout: 3000
    });
    const data = response.data;
    if (data && data.hasOwnProperty('DATA')) {
      if (isLog) {
        status = `Size ${data.DATA.length} vào lúc ${new Date().getHours()}:${new Date().getMinutes()}`;
        console.log(`Size ${data.DATA.length} vào lúc ${new Date().getHours()}:${new Date().getMinutes()}`);
        isLog = false;
      }
      const recordsWithLink = data.DATA.filter(record => record.buttonLink);
      if (recordsWithLink.length > 0) {
        urls = recordsWithLink.map((item) => {
          return {
            buttonLink: createNewUrl(item),
            startDate: item.startDate,
            endDate: item.endDate,
            eventTimeSpan: item.eventTimeSpan
          };
        });
      } else {
        urls = [];
      }
    }
  } catch (error) {
    console.error('Error fetching data:', error);
  } finally {
    setTimeout(fetchDataFromApi, 1000);
  }
}

function createNewUrl(record) {
  if (record.hasOwnProperty("oid") && record.buttonLink.includes("prod")) {
    let langParam = record.buttonLink.includes('?lang=vi') ? 'lang=vi&' : '';
    let newUrl = `${record.buttonLink.split('?')[0]}?${langParam}oid=${record.oid}`;
    return record.buttonLink = newUrl;
  } else {
    return record.buttonLink;
  }
}

function clearProcessedUrlsPeriodically() {
  setInterval(() => {
    isLog = true;
  }, 10 * 60 * 1000);
}

clearProcessedUrlsPeriodically();
fetchDataFromApi();

app.listen(3000, () => {
  console.log('Server is up on 3000')
})


module.exports = app;