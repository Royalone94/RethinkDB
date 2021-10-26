// Module dependencies.
const express = require('express');
const path = require('path');
const logger = require('morgan');
const bodyParser = require('body-parser');
const redis = require('socket.io-redis');
const app = module.exports = express();
app.set('port', process.env.PORT || 3000);

const server = app.listen(app.get('port'), () => {
    console.log('Express server listening on port ' + app.get('port'));
});

const io_s = require('socket.io')(server);
io_s.adapter(redis({ host: '127.0.0.1', port: 6379 }));
const io = io_s.of('binanceNS');

const rethinkDBScript = require('./rethinkdb');
rethinkDBScript(io);

app.use(logger('dev'));
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));

app.use(express.static(path.join(__dirname, 'public')));

if (app.get('env') === 'development') {
    app.use((error, req, res, next) => {
        res.status(err.status || 500);
        res.render('error', { message: err.message, error });
    });
}

// production error handler
// no stacktraces leaked to user
app.use((err, req, res, next) => { res.status(err.status || 500); res.render('error', err); });

app.get('/', (req, res) => { res.sendFile(__dirname + '/views/index.html'); });
app.get('/socketclient', (req, res, next) => { res.send(Object.keys(io.connected)); });

io.on('connection', (socket) => {
    // console.log('One Client Connected....');
    socket.on('get-coin-prices', async (data) => {
        if ( data.type === 'initialize' ){
            var arrCoinInfos = rethinkDBScript.getCurrentPrices();
            socket.emit('initialized-symbol-prices', { arrResult: arrCoinInfos });
        }else if ( data.type === 'update' ){
            var arrSymbols = data.symbols;
            if ( arrSymbols.length < 1 ) {
                socket.emit('updated-symbol-prices', { arrResult: [] });
                return;
            }
            var arrCoinInfos = rethinkDBScript.getCurrentPrices(arrSymbols);
            socket.emit('updated-symbol-prices', { arrResult: arrCoinInfos });
        }

        // io.emit('refreshedBTC', 'Received your Event...')
    })

    // socket.on('updateBTC', async (data) => {
    //     if ( data.type === 'refreshAgain') {
    //         var result = await getBTCInfo();
    //         io.emit('refreshedBTC', result);
    //     }
    // });
});

