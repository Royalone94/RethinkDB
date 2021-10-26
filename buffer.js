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

  

module.exports.setupRethinkDB = function ( funcCallback ){
  rethinkdb.connect({host: 'localhost', port: 28015}, function (err, conn) {
    //if (err) throw err;
    rethinkdb.dbCreate(DbName).run(conn, function (err, result) {
      // if (err) { console.log("---->Database '%s' already exists (%s:%s)\n%s", DbName, err.name, err.msg, err.message); }
      // else { console.log("---->Database '%s' created", DbName); }
      connection = conn;
      createTable(tblBitcoin);
      funcCallback();
    });
  });

  const createTable = function ( strTableName ){
    rethinkdb.db(DbName).tableCreate(strTableName).run(connection, async function(err, result) {
      if (err) {
        console.log( 'There was a table named ', strTableName, ' already.' );
        // throw err;
      }
      if ( strTableName === tblBitcoin ){
        await initializeCurrentBTCInfos();
        retrieveLiveBTCInfo();
      }
    })  
  }

  const initializeCurrentBTCInfos = async function (){
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

  const retrieveLiveBTCInfo = async function (){
    var objLiveBTCInfo = {}, arrLiveBTCInfo = [], curInterval = btcInterval;
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

  const updateBitcoins = function ( arrLiveBTCInfo ){
    var newItems = [], changedItems = [];
    for ( var i = 0; i < arrLiveBTCInfo.length; i++){
      var curObj = arrLiveBTCInfo[i]
      let oppositeObjIndex = currentBTCInfos.findIndex(ele => { return ele.key === curObj.key; })
      var oppositeObj = currentBTCInfos[oppositeObjIndex]

      if ( oppositeObj && curObj.value === oppositeObj.value ) continue;

      if ( !oppositeObj ) newItems.push(curObj);
      else {
        oppositeObj.value = curObj.value;
        changedItems.push( curObj );
      }
    }
    insertBitcoinItems( newItems );
    replaceBitcoinItems( changedItems );
    // currentBTCInfos = Object.assign([], arrLiveBTCInfo);
  }

  const insertBitcoinItems = async function ( items ){
    if ( items.length < 1 ) return;
    rethinkdb.db(DbName).table(tblBitcoin).insert(items)
      .run(connection, function(err, result) {
        if (err) { console.log('Error Occured when inserting coin infors...') }
        else { currentBTCInfos = currentBTCInfos.concat(result); }
      })
  }

  const replaceBitcoinItems = async function ( items ){
    if ( items.length < 1 ) return;
    // console.log('Replacing ', items.length, ' items...')
    rethinkdb.expr(items).forEach(function(row) {
      // console.log(row);
      return rethinkdb.db(DbName).table(tblBitcoin).filter({key: row('key')}).replace(row);
    })
    .run(connection, function(err, res){
      console.log(JSON.stringify(res, null , 2));
    });
    // rethinkdb.db(DbName).table(tblBitcoin)
    //   .filter({key: item.key})
    //   .update({value: item.value})
    //   .run(connection, function (err, res){
    //     if ( err ){
    //       console.log('Error Occured when replacing bitcoin info.')
    //     } else {
    //       console.log('updated this item : ', res);
    //     }
    //   })
    // get(row('id'))
  }

}

module.exports.registerChangeFeedsForBitcoins = function ( funcCallback ){
  var feedTable = tblBitcoin;
  rethinkdb.connect({db: DbName}).then(function(conn) {
    rethinkdb.table( feedTable ).changes().run(conn)
      .then(function(cursor) {
        console.log('CURSOR..........');
        cursor.each(function(err, item) {
          if ( err ) console.log('Error Occured when observing feedchaning of bitcoin...')
          else funcCallback(item);
        });
      });
  });
}