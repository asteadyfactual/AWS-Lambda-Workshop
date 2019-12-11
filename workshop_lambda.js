const AWS = require('aws-sdk');
const athena = new AWS.Athena();
const s3 = new AWS.S3();

// TODO edit this to handle your API parameters
function buildQuery(params) {
  if (!params) {
    return `select * from places limit 10`;
  }

  return `select * from places limit 10`;
}

exports.handler = async (event) => {
  const query = buildQuery(event.queryStringParameters);

  const startParams = {
    QueryString: query,
    QueryExecutionContext: {
      Database: 'workshop_db'
    },
    ResultConfiguration: {
      OutputLocation: 's3://aws-workshop-dec-2019-query-results/austin/' // TODO edit this
    }
  };

  const queryResults = await athena.startQueryExecution(startParams, function(err, data) {
    if (err) console.log(err);
    else {
      console.log(data);
      return data.QueryExecutionId;
    }
    return null;
  }).promise();

  const resultsParams = {
    QueryExecutionId: queryResults.QueryExecutionId,
    MaxResults: 100
  };

  const queryStatus = await checkIfExecutionCompleted(queryResults.QueryExecutionId);

  const s3Output =
        queryStatus.QueryExecution.ResultConfiguration
        .OutputLocation,
        statementType = queryStatus.QueryExecution.StatementType;

  let results = {};
  if (/.txt/.test(s3Output) || /.csv/.test(s3Output)) {
    results.Items = await getQueryResultsFromS3({
      s3Output,
      statementType
    });
  }

  const response = {
    statusCode: 200,
    body: results.Items
  };
  return response;
};

// IGNORE EVERYTHING BELOW THIS LINE

async function getQueryResultsFromS3(params) {
  const s3Params = {
    Bucket: params.s3Output.split("/")[2],
    Key: params.s3Output
      .split("/")
      .slice(3)
      .join("/")
  };

  const results = await s3.getObject(s3Params, function(err, data) {
    if (err) console.log(err);
    else {
      return data;
    }
    return null;
  }).promise();

  const input = results.Body.toString('utf-8');

  return input;
}

function isCommonAthenaError(err) {
  return err === "TooManyRequestsException" ||
    err === "ThrottlingException" ||
    err === "NetworkingError" ||
    err === "UnknownEndpoint"
    ? true
    : false;
}

function checkIfExecutionCompleted(QueryExecutionId) {
  let retry = 200;
  return new Promise(function(resolve, reject) {
    const keepCheckingRecursively = async function() {
      try {
        let data = await athena
            .getQueryExecution({ QueryExecutionId })
            .promise();
        if (data.QueryExecution.Status.State === "SUCCEEDED") {
          retry = 200;
          resolve(data);
        } else if (data.QueryExecution.Status.State === "FAILED") {
          reject(data.QueryExecution.Status.StateChangeReason);
        } else {
          setTimeout(() => {
            keepCheckingRecursively();
          }, retry);
        }
      } catch (err) {
        if (isCommonAthenaError(err.code)) {
          retry = 2000;
          setTimeout(() => {
            keepCheckingRecursively();
          }, retry);
        } else reject(err);
      }
    };
    keepCheckingRecursively();
  });
}
