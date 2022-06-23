const WebSocket = require("ws");
const mongoose = require("mongoose");
const _ = require("lodash");

///////////////////////Classes/////////////////////////
class PriceObj {
  constructor(name, price, time) {
    this.name = name;
    this.price = price;
    if (time) {
      this.minute = time.minute;
      this.hour = time.hour;
      this.day = time.day;
      this.month = time.month;
      this.year = time.year;
    }
  }
}
//////////////////////Functions///////////////////////
let updatePrice = (priceObj, price) => {
  priceObj.price.closing = price;
  if (priceObj.price.high < price) {
    priceObj.price.high = price;
  }
  else if (price.price.low > price) {
    priceObj.price.low = price;
  }
}


let getUTCTime = (time = new Date()) => {
  return ({
    time: {
      // second: (!time.getUTCSeconds())? 0: time.getUTCSeconds(),
      minute: (!time.getUTCMinutes())? 0: time.getUTCMinutes(),
      hour: (!time.getUTCHours())? 0 : time.getUTCHours()
    },
    date: {
      month: time.getUTCMonth() + 1,
      day: time.getUTCDate(),
      year: time.getUTCFullYear()
    }
  });
}

////////////////Database Schemas/Models///////////////////

mongoose.connect("mongodb://localhost:27017/cryptoDB", {
  useNewUrlParser: true,
  useUnifiedTopology: true
});

const minPriceSchema = new mongoose.Schema({
  name: String,
  price: {
    open: Number,
    high: Number,
    low: Number,
    close: Number
  },
  hour: Number,
  minute: Number,
  month: Number,
  day: Number,
  year: Number
  // startTime: {
  //   time: {
  //     second: Number,
  //     minute: Number,
  //     hour: Number
  //   },
  //   date: {
  //     month: Number,
  //     day: Number,
  //     year: Number
  //   }
  // },
  // lastCheckedTime: {
  //   time: {
  //     second: Number,
  //     minute: Number,
  //     hour: Number
  //   },
  //   date: {
  //     month: Number,
  //     day: Number,
  //     year: Number
  //   }
  });

const MinPriceData = mongoose.model("MinPriceData", minPriceSchema);

///////////////////Variables/Constants///////////////////////
const coinList = ["XBT/USD"] //coins being tracked
const krakenurl = "wss://ws.kraken.com";
let webSocketErrorCount = 0;
let lastMessageTime = null;
let letMessageClosePrice = null;
let currentMinPriceObj = null;
let currentPriceTime = null;
let isAlive;
let lastTime;
let lastClose;
const subMsg = { //initial subscription message
  event: "subscribe",
  pair: coinList,
  subscription: {
    interval: 1,
    name: "ohlc"
  }
}
///////////////////////Main//////////////////////////////////
let startWebSocket = () => {
  let isShuttingDown = false;
  let webSock = new WebSocket(krakenurl);
  webSock.onopen = () => {
    webSock.send(JSON.stringify(subMsg));
    //isAlive = setTimeout(restartWebSock, 240000);
  }

  let restartWebSock = () => {
    if (!isShuttingDown) {
      isShuttingDown = true;
      setTimeout(startWebSocket, 2000);
      console.log(`Connection Error: Connection "Timed Out" ${JSON.stringify(getUTCTime())}`);
      webSock.close(4950, "Closed due to no new data being received within 4 minutes. Will attempt to restart");
    } else {
      console.log("No new data has been received but the server is already restarting");
    }
  }

  webSock.onerror = error => {
    console.log(`WebSocket error: ${error}`);
  }

  webSock.onmessage = e => {
    clearTimeout(isAlive);
    const message = JSON.parse(e.data);
    if (!message.event) {
      // console.log(`Start Time ${JSON.stringify(getUTCTime(new Date(1000*parseFloat(message[1][0]))).time)} +
      // End Time ${JSON.stringify(getUTCTime(new Date(1000*parseFloat(message[1][1]))).time)} O ${parseFloat(message[1][2])} H ${parseFloat(message[1][3])} L ${parseFloat(message[1][4])} C ${parseFloat(message[1][5])}`);
      // console.log(JSON.stringify(message));
      // console.log(message);
      let milliSec = 1000*parseFloat(message[1][1]) - 60000;
      let currentTime = getUTCTime(new Date(milliSec));
      if (currentTime.time.minute === null) {
        currentTime.time.minute = 1;
      }
      if (currentTime.time.hour === null) {
        currentTime.time.hour = 1;
      }
      if (_.isEqual(currentTime, currentPriceTime)) {
        currentMinPriceObj.price = {
          open: parseFloat(message[1][2]),
          high: parseFloat(message[1][3]),
          low: parseFloat(message[1][4]),
          close: parseFloat(message[1][5])
        }
      }
      else {
        if (currentMinPriceObj != null) {
          console.log(currentMinPriceObj);
          const dbMinPriceItem = new MinPriceData(currentMinPriceObj);
          dbMinPriceItem.save((err) => {
            err ? console.log(err) : null
          });
          for (let i = lastTime + 60000; i < milliSec; i+= 60000) {
            let fillerTime = getUTCTime(new Date(i));
            const fillerDBPriceItem = new MinPriceData({
              name: coinList[0],
              price: {
                open: lastClose,
                high: lastClose,
                low: lastClose,
                close: lastClose
              },
                ...fillerTime.time,
                ...fillerTime.date
            });
            fillerDBPriceItem.save((err) => {
              err ? console.log(err) : null
            });
          }
        }
        currentMinPriceObj = new PriceObj(coinList[0],
        {
          open: parseFloat(message[1][2]),
          high: parseFloat(message[1][3]),
          low: parseFloat(message[1][4]),
          close: parseFloat(message[1][5])
        },
        {...currentTime.time, ...currentTime.date});
        currentPriceTime = currentTime;
        lastTime = milliSec;
      }
    lastClose = parseFloat(message[1][5]);
    }
    isAlive = setTimeout(restartWebSock, 240000);
  }
  webSock.onclose = data => {
    if (!isShuttingDown) {
      clearTimeout(isAlive);
      isShuttingDown = true;
      console.log(`WebSocket tried to close at this UTC date object ${JSON.stringify({...getUTCTime(), seconds: new Date().getUTCSeconds()})} with ${webSocketErrorCount} many
      current attempted Closes `);
      console.log(data);
      webSocketErrorCount++;
      setTimeout(startWebSocket, 2000);
    }
  }
}

startWebSocket();
