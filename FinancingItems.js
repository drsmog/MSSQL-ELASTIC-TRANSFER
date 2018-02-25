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

    await createFinanceItemIndex()
    console.time('st');
    connectionPool = await new sql.ConnectionPool(config).connect()


    for (let offset = 0; offset < 5000; offset++) {
        let financeItems = await fetchdata(offset);
        if (!financeItems || financeItems.length === 0) {
            console.log('data transfer done');
            console.timeEnd('st');
            process.exit(0);
        }
        await elasticFinanceItemBulkInsert(financeItemIndexName, financeItemTypeName, financeItems);
        console.log('fetch data offset:', offset, 'dataset:', financeItems.length);
    }

    process.exit(0);

}

async function createFinanceItemIndex() {
    // Delete if exists
    let exists = await client.indices.exists({ index: financeItemIndexName })
    if (exists === true) {
        await client.indices.delete({ index: financeItemIndexName })
    }

    // Create Index
    const indexDefaultOptions = {
        'settings': {
            'index': {
                'number_of_shards': 1,
                'number_of_replicas': 1
            }
        },
        // "mappings": {
        //     "financeitems": {
        //         "properties": {
        //             "tags": {
        //                 "type": "text",
        //                 "fields": {
        //                     "keyword": {
        //                         "type": "keyword",
        //                         "ignore_above": 256
        //                     }
        //                 }
        //             },
        //             "FinancingItemClassifications": {
        //                 "type": "text",
        //                 "fields": {
        //                     "keyword": {
        //                         "type": "keyword",
        //                         "ignore_above": 256
        //                     }
        //                 }
        //             },
        //             "tags": {
        //                 "type": "text",
        //                 "fields": {
        //                     "keyword": {
        //                         "type": "keyword",
        //                         "ignore_above": 256
        //                     }
        //                 }
        //             },
        //             "tags": {
        //                 "type": "text",
        //                 "fields": {
        //                     "keyword": {
        //                         "type": "keyword",
        //                         "ignore_above": 256
        //                     }
        //                 }
        //             }
        //         }
        //     }
        // },
    }

    await client.indices.create({
        index: financeItemIndexName,
        body: indexDefaultOptions
    })


}

async function fetchdata(offset) {

    try {
        let fromrow = offset * 5000;
        const baseQuery = `
        select 
            *
            from FinancingItems fit 
            order by fit.FinancingItemId
            OFFSET ${fromrow}  ROWS FETCH NEXT 5000 ROWS ONLY 
        `
        const request = new sql.Request(connectionPool)
        const result = await request.query(baseQuery)
        const financingItems = result.recordset
        return financingItems;
    } catch (err) {
        // ... error checks
        console.log(err);
    }

}

async function elasticFinanceItemBulkInsert(index, type, financeItems) {
    let bulk = []
    let action = { index: { _index: index, _type: type } }

    financeItems.forEach(item => {
        action.index._id = item.FinancingItemId
        item.FinancingItemClassifications = []
        item.FinancingItemMeasurementUnits = []
        item.FinancingItemAttributeSummaries = []
        bulk.push(action, item)
    })
    await client.bulk({
        body: bulk
    })
}

