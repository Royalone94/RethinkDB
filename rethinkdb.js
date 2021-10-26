var rethinkdb = require('rethinkdb');

var connection = null;

const DbName = 'smarttrade';
const tblBitcoin = 'tb_bitcoins';

var updateBitcoin_Timeout = null;

var currentBTCInfos = [];

// -----------------------------------------------
const Binance = require('binance-api-node').default
const client = Binance();
// async function getBTCInfo(){
//     var result = [];
//     await client.prices()
//         .then( res => { result = Object.assign({}, res ); })
//         .catch( err => { console.log('There was a connection problem with Binance API...'); })
//     return result;
// }
// -----------------------------------------------
var failureCount = 0, btcInterval = 1000, btc_timeout = null;

module.exports = function (io) {

  rethinkdb.connect( {host: 'localhost', port: 28015}, function(err, conn) {
      if (err) {
        console.log('There was a connection problem while connecting RethinkDB....');
        throw err;
      } else {
        connection = conn;
        createDB(DbName);
      }
  });

  function changeFeeds( feedTable ){
    rethinkdb.connect({db: DbName}).then(function(c) {
      // rethinkdb.table(tblBitcoin).filter(rethinkdb.row("id")(0).eq("cluster")).changes().run(c)
      rethinkdb.table( feedTable ).changes().run(c)
        .then(function(cursor) {
          cursor.each(function(err, item) {
            // io.emit("updated-symbol-prices", item);
          });
        });
    });

    retrieveLiveBTCInfo();
  }

  function createDB( strDBName ){
    rethinkdb.dbCreate( strDBName ).run(connection, function(err, status){
      if (err) {
        console.log( 'There was a database named ', strDBName, ' already.' );
        // throw err;
      }
      createTable(tblBitcoin);
    });
  }

  function createTable( strTableName ){
    rethinkdb.db(DbName).tableCreate(strTableName).run(connection, async function(err, result) {
      if (err) {
        console.log( 'There was a table named ', strTableName, ' already.' );
        // throw err;
      }
      if ( strTableName === tblBitcoin ){
        await initializeCurrentBTCInfos();
        changeFeeds(strTableName);
      }
    })  
  }

  async function initializeCurrentBTCInfos(){
    var result = [];
    await rethinkdb.db(DbName).table(tblBitcoin).run(connection, function(err, cursor) {
      if (err) {
        console.log('Error Occured when initialize Bitcoin infos.');
      } else{
        cursor.toArray(function(err, result) {
          if (err) {
            console.log('Error Occred when retrieing initialize Bitcoin infos...')
          } else {
            currentBTCInfos = Object.assign([], result);
          }
        });
      }
    });
  }

  async function retrieveLiveBTCInfo(){
    var objLiveBTCInfo = {}, arrLiveBTCInfo = [], curInterval = btcInterval;
    // var resPing = await client.ping();
    // console.log('Result Ping :', resPing);
    // await client.trades({ symbol: 'ETHBTC' })
    await client.prices()
      .then( res => { objLiveBTCInfo = Object.assign({}, res ); })
      .catch( err => { console.log('There was a connection problem with Binance API...'); })

    var arrKeys = Object.getOwnPropertyNames(objLiveBTCInfo);

    for ( var i = 0; i < arrKeys.length; i++){
        var key = arrKeys[i];
        var value = objLiveBTCInfo[key];
        var curBTCObj = { 'key': key, 'value' : value }
        arrLiveBTCInfo.push( curBTCObj );
      }

    if ( arrLiveBTCInfo.length > 0 ) {
      failureCount = 0;
      updateBitcoins(arrLiveBTCInfo);
    }
    else failureCount++;

    if ( failureCount > 10 ) curInterval *= 60 * 5;
    
    console.log('retrieving from binance-api ...  retrieved BTC Count : ', arrLiveBTCInfo.length, ' FailureCount : ', failureCount );

    clearTimeout(btc_timeout);
    btc_timeout = setTimeout(function () { retrieveLiveBTCInfo(); }, curInterval );
  }

  function updateBitcoins( arrLiveBTCInfo ){
    var newItems = [];
    // var changedItems = [];
    for ( var i = 0; i < arrLiveBTCInfo.length; i++){
      var curObj = Object.assign({}, arrLiveBTCInfo[i])
      var oppositeObjIndex = currentBTCInfos.findIndex(ele => { return ele.key === curObj.key; })
      var oppositeObj = currentBTCInfos[oppositeObjIndex]

      if ( oppositeObj && curObj.value === oppositeObj.value ) continue;

      if ( !oppositeObj ){
        newItems.push(curObj);
        currentBTCInfos.push( curObj );
      } else {
        currentBTCInfos[oppositeObjIndex].value = curObj.value;
        // changedItems.push( curObj );
        replaceBitcoinItem( curObj );
      }
    }
    insertBitcoinItems( newItems );
  }

  async function insertBitcoinItems( items ){
    if ( items.length < 1 ) return;
    rethinkdb.db(DbName).table(tblBitcoin).insert(items)
      .run(connection, function(err, result) {
        if (err) { console.log('Error Occured when inserting coin infors...') }
        // else { currentBTCInfos = currentBTCInfos.concat(result); }
      })
  }

  async function replaceBitcoinItem( item ){
    /*
      COMMENT(2018.07.31) : SHADOW
      Multile update is required
    */

    if ( !item.key ) return;
    rethinkdb.db(DbName).table(tblBitcoin).filter({ key: item.key}).update({ value: item.value })
      .run(connection, function(err, res){
        if ( err ) console.log('Replacing... ', err)
        // else console.log(JSON.stringify(res, null , 2));
      });
  }
};

module.exports.getCurrentPrices = function( arrSymbols = null ){
  if ( !arrSymbols || arrSymbols.length < 1 ){
    return currentBTCInfos;
  } else {
    var arrResult = [];
    for ( var i = 0; i < arrSymbols.length; i++ ){
      var curSymbol = arrSymbols[i];
      var curIndex = currentBTCInfos.findIndex(ele => {
        return ele.key === curSymbol;
      });
      if ( curIndex < 0 ) continue;
      var curObj = { key: curSymbol, value: currentBTCInfos[curIndex].value };
      arrResult.push(curObj);
    }
    return arrResult;
  }
}