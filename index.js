const express = require('express')
const axios = require('axios');
const mysql = require('mysql2');
const cors = require('cors');
const app = express()
app.use(cors());
app.use(express.json());
const apiUrl = 'https://www.goethe.de/rest/examfinder/exams/institute/O%2010000611?category=E006&type=ER&countryIsoCode=&locationName=&count=24&start=1&langId=134&timezone=54&isODP=0&sortField=startDate&sortOrder=ASC&dataMode=0&langIsoCodes=de%2Cen%2Cvi';
let urls = [];
let isLog = true;
let status = '';
let oldDataLength = 0;
let isInserting = true;

const dbConfig = {
  host: '103.200.23.80',
  user: 'herokuap_tudt',
  password: 'Agglbtpg123',
  database: 'herokuap_tudt',
  waitForConnections: true,
  connectionLimit: 10000,
  queueLimit: 0,
  connectTimeout: 180000
};

const pool = mysql.createPool(dbConfig);

async function insertUrlsData(urlsData) {
  if (!isInserting) {
    return;
  }
  try {
    if (urlsData.length === 0 && oldDataLength !== 0) {
      await deleteAllData();
      oldDataLength = 0;
    } else if (urlsData.length > 0) {
      const connection = await pool.promise().getConnection();
      try {
        await connection.beginTransaction();
        await deleteAllData();
        const insertQuery = `
          INSERT INTO urls (buttonLink, startDate, endDate, eventTimeSpan)
          VALUES ?
        `;
        const values = urlsData.map(url => [url.buttonLink, url.startDate, url.endDate, url.eventTimeSpan]);
        await connection.query(insertQuery, [values]);
        oldDataLength = urlsData.length;
        await connection.commit();
      } catch (error) {
        await connection.rollback();
        throw error;
      } finally {
        isInserting = false;
        connection.release();
      }
    } else {
      return;
    }
  } catch (error) {
    throw error;
  }
}

app.get('/api/get-btn', async (req, res) => {
  try {
    const connection = await pool.promise().getConnection();
    try {
      const query = 'SELECT * FROM urls ORDER BY RAND() LIMIT 1';
      const [rows] = await connection.query(query);
      if (rows.length === 0) {
        return res.json(null);
      }
      res.json(rows[0]);
    } finally {
      connection.release();
    }
  } catch (error) {
    console.error('Lỗi truy vấn cơ sở dữ liệu:', error);
    res.status(500).json({ error: 'Lỗi truy vấn cơ sở dữ liệu' });
  }
});


async function deleteAllData() {
  const connection = await pool.promise().getConnection();
  try {
    await connection.beginTransaction();
    const deleteQuery = 'DELETE FROM urls';
    await connection.query(deleteQuery);
    await connection.commit();
  } catch (error) {
    await connection.rollback();
    throw error;
  } finally {
    connection.release();
  }
}

app.post('/api/insert-urls', async (req, res) => {
  const urlsData = req.body.urls;
  try {
    const result = await insertUrlsData(urlsData);
    res.json({ success: true, rowsAffected: result.affectedRows });
  } catch (error) {
    res.status(500).json({ error: 'Lỗi thêm dữ liệu' });
  }
});

app.get('/api/get-status', async (req, res) => {
  try {
    res.json(status);
  } catch (error) {
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
        await insertUrlsData(urls)
      } else {
        urls = [];
        await insertUrlsData([])
      }
    }
  } catch (error) {
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

async function clearProcessedUrlsPeriodically() {
  await deleteAllData();
  setInterval(() => {
    isLog = true;
  }, 10 * 60 * 1000);
  setInterval(() => {
    isInserting = true;
  }, 2000);
}

clearProcessedUrlsPeriodically();
fetchDataFromApi();

app.listen(3000, () => {
  console.log('Server is up on 3000')
})


module.exports = app;