'use strict';
var fs = require('fs');

var mongo = require('mongoskin');
var URLlib = require('url-parse');
var qs = require('querystring');
var S = require('string');
var request = require('request');
var bignumJSON = require('json-bignum');

//print the command line..
console.log(process.argv.slice(2)[0]);

//get the first arg as config file
var configFile = process.argv.slice(2)[0];
var config = JSON.parse(fs.readFileSync(configFile, 'utf8'));
var db = mongo.db('mongodb://localhost:27017/' + config.Name);
var authObject;

//Logger function
function log(func, text) {
	console.log(func + ': ' + text);
}

function createRequest(url, callback) {
	request(url.toString().trim(), function (error, response, body) {
		if(error) {
			
			log('\nRequest Error', error);
			//callback(error, '');
			process.exit(-1);
		}

		try {
		  //log(body);
		  callback(error, JSON.parse(body));
		} catch (error) {
			callback(error, body);
		}
	});
}

function getParamValue(param, valFunction) {

	if (param.indexOf('@Config.Results') > -1 && authObject) {

		var key = param.substring(param.indexOf('@Config.Results') + 16, param.length - 1);

			//console.log(key + ' ' + authObject);
			
			if (authObject[key]) {
				valFunction(authObject[key]);
			} else if(authObject) {
				log('getParamValue', 'object lost parsing again...: ' + authObject);
				authObject = JSON.parse(authObject);
				
				valFunction(authObject[key]);
			} else {
				
				log('getParamValue', 'Undefined Key ' + key + ' in ' + authObject);
				process.exit(-1);
			}
		} else if (param.indexOf('@Config') > -1 && authObject) {
			var result = config;
			
			for(var p = 1 ; p < param.split('.').length ; p++)
			{
				var field2 = param.split('.')[p].replace(']', '');
				
				result = result[field2];
			}
			
			console.log('config item', result);
			valFunction(result);
		}
		else
		{
			valFunction(param);
		}
	}
	
	function appendParams(URL, params, resultWithParamsFunc)	{

		for (var key in params) 
		{
				//console.log(authObject + ' ' + key);
				if(params[key].indexOf('[@') > -1 && authObject) {
					var k = params[key].substring(params[key].indexOf('@Config.Results') + 16, params[key].length - 1);

					var val = authObject[k];
					
					if (val) {
						URL = URL + key + '=' +  val.replace('"', '').replace('"','') + '&';
					} else {
						
						val = authObject[k];
						
						URL = URL + key + '=' +  val.replace('"', '').replace('"','') + '&';
					}
				} else {
					
					URL = URL + key + '=' + params[key] + '&';
				}
				
			}

		//Remove the last & char
		if (URL.substring(URL.length-1) == '&') {
			URL = URL.substring(0, URL.length-1);
		}
		
		resultWithParamsFunc(URL);
	}
	
	// Get URL with @Table params and extract all to json array
	function extractURLTables(URL, returnJsonTablesfunc)	{
		var jsonResult = []; //Table, field, ParamsLength
		
		while(URL.indexOf('[@Table') > -1) //extract the table values
		{

			var tableItems = URL.split('.').length;
			
			var pos = URL.indexOf('[@Table');
			
			var last = URL.indexOf(']');
			
			var param = URL.substring(pos, last+1);

			var table = param.split('.')[1];

			// var items = param.split('.').length;
			
			//log('param', URL);
			var field = param.split('.')[2].replace(']', '');
			
			var resTable = {
				'param' : param,
				'table': table,
				'field': field,
				'items' : tableItems
			};

			jsonResult.push(resTable);
			
			URL = URL.substring(last+1, URL.length);
		}
		
		returnJsonTablesfunc(jsonResult);
		
	}
	
	
	
	function appendTableParams(URL, tablesArray, doc,tableKeyVals, returnFixedTable)	{
		for(var i = 0; i < tablesArray.length ; i++) {
			var field = tablesArray[i].field;

			var param = tablesArray[i].param;
			
			var docValue = doc[field];
			//if field is complex then get value
			if (docValue && typeof docValue === 'object') {
				docValue = doc;
				
				//log('appendTableParams', param.split('.').length);
				
				for(var p = 2 ; p < param.split('.').length ; p++) {
					var field2 = param.split('.')[p].replace(']', '');
					
					docValue = docValue[field2];
				}
			} else {
				docValue = doc[field];
			}

			//console.log('docValue', docValue);
			if(docValue) {
				var param2;
				if(docValue.numberStr)
					param2 = docValue.numberStr;
				else	
					param2 = docValue.toString();

				getParamValue(param2, function (paramVal) {
					//Clean the key and set the val
					var keyval = {'key' : param.replace('[@Table.', '').replace(/\./g,'_').replace(']', ''), 'value' : paramVal.replace('"', '').replace('"','')};
					
					tableKeyVals.push(keyval);
					
					URL = URL.replace(param, paramVal.replace('"', '').replace('"', ''));

					if(tablesArray.length - 1 == i) {
						returnFixedTable(URL, tableKeyVals);
					}
				});
			} else {
				console.log('Cannot find field ' + field, JSON.stringify(doc));
			}
		}
	}
	
	//Build the URL from the req
	function processURLParams(URL,params,keyVals, resultURLFunc)	{
		//console.log('process URL func : ' + URL);
		var resultURL = URL;

		var results = [];

		//Assign all parameters
		if(resultURL.indexOf('[@Config') > -1) {
			var pos = resultURL.indexOf('[@');
			
			var last = resultURL.indexOf(']');
			
			var param = resultURL.substring(pos, last+1);

			getParamValue(param, function(paramVal)
			{		
				
				resultURL = resultURL.replace(param, paramVal.replace('"', '').replace('"',''));

				processURLParams(resultURL,params,keyVals, resultURLFunc);
				
			});
		//extract the table values 
	} else if(resultURL.indexOf('[@Table') > -1) {
		extractURLTables(resultURL, function(tablesArray) {
				//get all items within the table and push it to results
				db.collection(tablesArray[0].table).find().toArray(function (err, docs) {
					var i = 0;

					//for each record found fill the params with doc values
					docs.forEach(function (doc) 	{
						//log('processURLParams', param + ' field: ' +  field + ' data: ' + doc[field]);
						appendTableParams(URL, tablesArray, doc, keyVals, function(URL, tableKeyVals)
						{
							//add the required config params to the URL
							appendParams(URL, params, function(UrlWithParams)
							{
								i++;
								
								// push to results 
								results.push(UrlWithParams);
								
								if (docs.length == i) {

									resultURLFunc(results, tableKeyVals);
								}
							});
						});
					});
				});
			});
	}
	else
	{
			//add one item to the array
			results.push(resultURL);

			appendParams(results[0], params, function(UrlWithParams)
			{
				//console.log('UrlWithParams--' + params);
				results[0] = UrlWithParams;
				resultURLFunc(results, keyVals);
			});
		}
	}

	function getAuthKeys(data, doneFunction) {
		var authURL = data.Config.Auth_Url;

		var keyVals = [];
		if(authURL.length === 0){
			doneFunction('',data.Config.Params );
		} else {
			processURLParams(authURL, data.Config.Params, keyVals, function(resultURL)
			{	
				console.log('\n' + resultURL[0]);
				createRequest(resultURL[0], function(err, body)
				{	
					doneFunction(err, body);
				});
			});
		}
	}



	//
	function runWebQuery(URL, table, tableKeyVals, processResultsDoneFunc)	{
		console.log('Run Query:\n' + URL);

		createRequest(URL, function (err, body) {
			var resultItem;

			if(typeof body =='object') {
			  //It is JSON
			  resultItem = body;
			} else {
				try {
					resultItem = bignumJSON.parse(body);	
				} catch(err) {
					console.log('Error Parsing result:\n' + err);
					processResultsDoneFunc('', '');  
					return;
				}
			}
			
			var dbTable = db.collection(table.Name);

			log('dbTable', table.Name);
			//var dataItems = resultItem;
			//find the results node
			if(table.DataPath) {
				var depthItems = table.DataPath.split('.');
				
				for(var i = 0; i < depthItems.length ; i++) {
					resultItem = resultItem[depthItems[i]];
				}

			}
			
			var nextPage;
			
			//check next page
			if(table.Pages) {
				if(table.Pages.type === 'RANGE') {
					//take the URL and switch the range prams according to the chuck size
					var newURLOBJ = new URLlib(URL);
					
					// var startParam = S(newURLOBJ.query).between(table.Pages.paramStart + '=', '&');
					
					// var endParam = S(newURLOBJ.query).between(table.Pages.paramEnd + '=', '&');
					
					var chunk = parseInt(table.Pages.chunkSize);

					var paramsObj = qs.parse(newURLOBJ.query.replace('?', ''));
					
					var startRange = parseInt(paramsObj[table.Pages.paramStart]);

					var endRange = parseInt(paramsObj[table.Pages.paramEnd]);
					
					var newStartRange = startRange + chunk;

					var newEndRange   = endRange + chunk;
					
					//set new params
					paramsObj[table.Pages.paramStart] = newStartRange;
					
					paramsObj[table.Pages.paramEnd] = newEndRange;
					
					newURLOBJ.query = qs.stringify(paramsObj);
					
					nextPage = newURLOBJ.toString();

				} else if(table.Pages.type == 'URL') {
					var depthItems = table.Pages.nextPageURL.split('.');
					
					nextPage = bignumJSON.parse(body);
					
					if(nextPage) {
						for(var i = 0; i < depthItems.length ; i++) {
							if(nextPage)
								nextPage = nextPage[depthItems[i]];
						}
						
						if(nextPage && nextPage.substr(nextPage.length - 1) != '&')
							nextPage = nextPage + '&';
					}
					
					// in case we did not get full URL build it again
					if(nextPage && 	table.Pages && table.Pages.keepURL && table.Pages.keepURL == 'true') {

						//Remove the last & char
						if (nextPage.substring(nextPage.length-1) == '&') {
							nextPage = nextPage.substring(0, nextPage.length-1);
						}
						
						console.log('Next Param: ', nextPage);
						
						var whatToReplace = table.Pages.nextPageURL;

						var replaceMe;
						var urlObj;
						
						//append the next page param if does not exit and append param equals true
						if(table.Pages.appendParam == 'true' && URL.indexOf(table.Pages.nextPageURL) == -1) { 
							URL += '&' + table.Pages.nextPageURL + '=XXXXXX&';
							
							//console.log('append item:' + URL);
							
							urlObj = new URLlib(URL);

							replaceMe = 'XXXXXX';
						} else {
							urlObj = new URLlib(URL);

							replaceMe = S(urlObj.query).between(whatToReplace + '=', '&');
						}

						urlObj.query = urlObj.query.replace(replaceMe, nextPage);	
						
						console.log('New URL \n ' + urlObj.query);
						
						nextPage = urlObj.toString();

					}
				}
				//console.log('next URL:\n', nextPage);
			}
			


			// add the additional data to the json object	
			if(tableKeyVals && resultItem) {
				
				//console.log(resultItem.length);
				for(var i = 0 ; i < resultItem.length ; i++) {
					resultItem[i][tableKeyVals.key] = tableKeyVals.value;
				}

			} else if(!resultItem) {
				log('No results','');
				processResultsDoneFunc('', '');  
			}

			insertDocuments(dbTable, resultItem, function (err, doc) {				
				
				if (err) {
					log('dbTable.insert', 'Error inseting: ' + err);
					processResultsDoneFunc(err, doc);  
				} else {
					//TODO: verify valid URL now I'm using > 7 
					if(nextPage && nextPage.length > 7) {
						
						var keyVals = [];
						
						//Keep URL does not require appending params
						if(table.Pages.keepURL && table.Pages.keepURL == 'true') {
							runWebQuery(nextPage, table, null, function (err, doc) 	{
								processResultsDoneFunc(err, doc); 
							});
						} else {
							processURLParams(nextPage, table.Params,keyVals, function (resultURLs, returnKeyVals) {

								runWebQuery(resultURLs, table, returnKeyVals, function (err, doc) {
									
									processResultsDoneFunc(err, doc); 
								});
							});
						}
					} else {
						//console.log('6'); 
						processResultsDoneFunc(err, doc); 
					}						
				}
			});
		});
}

function processURLS(resultsArray, table, tableKeyVals, returnFunc) {
	if (resultsArray.length > 0) {
		var currentURL = resultsArray[0];

		var currentKeyVal = tableKeyVals[0];

			resultsArray.shift(); // remove the first item
			
			tableKeyVals.shift();
			
			runWebQuery(currentURL,table, currentKeyVal, function (err, doc) {	
				
				processURLS(resultsArray, table,tableKeyVals, returnFunc);
			});
		//no more results 
	} else {
		returnFunc();
	}
}

function processTable(table, processTableDoneFunc)	{
	var url = table.Base_Url;

		//clean Table before processing
		var dbTable = db.collection(table.Name);

		
		dbTable.remove({});

		var keyVals = [];
		//build the table params 
		processURLParams(url, table.Params,keyVals, function (resultURLs, returnKeyVals) {
			
			processURLS(resultURLs,table,returnKeyVals, function () {

				processTableDoneFunc();
			});

		});
	}

	
function processTables(tables, index, processDone)	{
	log('Table: ', index + ' of ' + tables.length);

	if (index >= tables.length) {
		processDone();
	} else {
		processTable(tables[index], function () {
			var y = index + 1;
			// Pause between calls to avoid API limitations
			setTimeout(function()
			{
				processTables(tables, y, processDone);
			}, processTimeout );
			processTimeout += 1000;
		});			 
	}
}


function insertDocuments(dbTable, docs, cb){
	if (!docs || (Array.isArray(docs) && docs.length===0)){
		cb();
	}else if (Array.isArray(docs) && docs.length>1){
			dbTable.insert(docs, cb);
	}else{
		var singleDoc;
		if (docs.length==1){
			singleDoc = docs[0];
		}else{
			singleDoc = docs;
		}

		if (singleDoc) {
			dbTable.insert(singleDoc, cb);
		}
	}
}


if(config)	{

	// var db = mongo.db('mongodb://localhost:27018/' + config.Name);

	var processTimeout = 0;

	//Start by getting auth item
	getAuthKeys(config, function (err, success) {
		authObject = success;

		processTimeout = parseInt(config.Config.TimeBetweenCalls);

		// set process timeout to 1000 (1 sec) if not set in config
		if(!processTimeout)
			processTimeout = 1000;
		
		processTables(config.Tables, 0, function () {
			log('processTables', 'Done');
			process.exit(0);
		});
	});	

}