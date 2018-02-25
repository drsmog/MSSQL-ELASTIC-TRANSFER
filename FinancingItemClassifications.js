//connectionString="Data Source=172.17.7.67;Initial Catalog=CitizenPortal;User ID=isvanidze;Password=A@123456"
const sql = require('mssql')
const elasticsearch = require('elasticsearch')
const client = new elasticsearch.Client({
    host: 'localhost:9200',
    log: 'error'
})

const financeItemIndexName = 'financeitems';
const financeItemTypeName = 'financeitems';

const config = {
    user: 'isvanidze',
    password: 'A@123456',
    server: '172.17.7.67',
    database: 'CitizenPortal',
    pool: {
        max: 2000,
        min: 0,
        idleTimeoutMillis: 30000000
    },
    connectionTimeout: 30000000,
    requestTimeout: 30000000
}
var connectionPool = undefined;

startFetchingData();


async function startFetchingData() {


    console.time('st');
    connectionPool = await new sql.ConnectionPool(config).connect()


    for (let offset = 0; offset < 5000; offset++) {
        let data = await fetchdata(offset);
        if (!data || data.length === 0) {
            console.log('data transfer done');
            console.timeEnd('st');
            process.exit(0);
        }
        await elasticFinanceItemBulkInsert(financeItemIndexName, financeItemTypeName, data);
        console.log('fetch data offset:', offset, 'dataset:', data.length);
    }

    process.exit(0);

}


async function fetchdata(offset) {

    try {
        let fromrow = offset*5000;
        const baseQuery = `
        select * from FinancingItemClassifications
        order by FinancingItemClassificationId
        OFFSET ${fromrow}  ROWS FETCH NEXT 5000 ROWS ONLY
        `
        const request = new sql.Request(connectionPool)
        const result = await request.query(baseQuery)
        return result.recordset;
    } catch (err) {
        // ... error checks
        console.log(err);
    }

}

async function elasticFinanceItemBulkInsert(index, type, data) {
    try {
        let bulk = []
        data.forEach(item => {
            let action = { update: { _id: item.FinancingItemId, "_type": "financeitems", "_index": "financeitems" } }
            let doc = {
                script: {
                    source: "ctx._source.FinancingItemClassifications.add(params.classification)",
                    lang: "painless",
                    params: { "classification": item }
                }
            }
            //{ "script" : {"source": "ctx._source.tags.add(params.tag)","lang": "painless","params" : {"tag" : "blue"}} }
            bulk.push(action, doc)
        })
        let r = await client.bulk({
            body: bulk
        })
        console.dir(r.items[0].update.error);
    } catch (error) {
        console.log(error)
    }


}

