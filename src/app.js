'use strict';
const http = require('http');
var assert = require('assert');
const express= require('express');
const app = express();
const mustache = require('mustache');
const filesystem = require('fs');
const url = require('url');
const port = Number(process.argv[2]);

const hbase = require('hbase')
var hclient = hbase({ host: process.argv[3], port: Number(process.argv[4])})

function rowToMap(row) {
	var stats = {}
	row.forEach(function (item) {
		stats[item['column']] = Number(item['$'])
	});
	return stats;
}

hclient.table('zhangfx_final_summary').row('AAME').get((error, value) => {
	console.info(rowToMap(value))
	console.info(value)
})


app.use(express.static('public'));
app.get('/price.html',function (req, res) {
    const stock=req.query['stock'];
    console.log(stock);
	hclient.table('zhangfx_final_summary').row(stock).get(function (err, cells) {
		try {
			const stockInfo = rowToMap(cells);
			console.log(stockInfo)
			function sharpe_ratio() {
				var baseReturn = (stockInfo["result:end_day_index"] - stockInfo["result:start_day_index"]) / stockInfo["result:start_day_index"] - 1;
				var stockReturn = (stockInfo["result:end_day_stock"] - stockInfo["result:start_day_stock"]) / stockInfo["result:start_day_stock"] - 1;
				var risk = stockInfo["result:value_std"]
				if(risk == 0)
					return " inf ";
				return ((stockReturn - baseReturn)/risk).toFixed(2); /* two decimal place */
			}

			var template = filesystem.readFileSync("result.mustache").toString();
			var html = mustache.render(template,  {
				stock : req.query['stock'],
				num_days: stockInfo["result:num_days"],
				ratio: sharpe_ratio(),
			});
			res.send(html);
		} catch (e) {
			console.log(e)
		}

	});
});
	
app.listen(port);
