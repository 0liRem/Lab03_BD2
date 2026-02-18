//          DOCUMENTACIÓN INTERNA
//
//  
//  Proyecto: Lab03
//  Programadores: 
//          Oli Viau 23544
//          Fabian Morales 23267
//
//  Recursos: 
//      https://www.mongodb.com/docs/manual/crud/
//      https://www.mongodb.com/docs/manual/reference/operator/aggregation/sort/
//      https://www.mongodb.com/docs/manual/reference/operator/aggregation/unwind/
//      https://www.mongodb.com/docs/manual/reference/operator/aggregation/lookup/
//      
//

require('dotenv').config();
const { MongoClient, ServerApiVersion } = require('mongodb');
const fs = require('fs').promises;

const uri = process.env.MONGODB_URI;

const client = new MongoClient(uri, {
  serverApi: {
    version: ServerApiVersion.v1,
    strict: false,
    deprecationErrors: true,
  }
});

async function query() {
  try {
    await client.connect();
    //Base de gatos
    const database = client.db("sample_analytics");
    const customers = database.collection("customers");
    const accounts = database.collection("accounts");
    const transactions = database.collection("transactions");

    // 2.1 Número total de transacciones y monto promedio por cliente
    console.log("\n 2.1 Total de transacciones y monto promedio por cliente:");
    const query21 = await customers.aggregate([
      { $unwind: "$accounts" },
      {
        $lookup: {
          from: "transactions",
          localField: "accounts",
          foreignField: "account_id",
          as: "account_transactions"
        }
      },
      { $unwind: "$account_transactions" },
      { $unwind: "$account_transactions.transactions" },
      {
        $group: {
          _id: "$_id",
          nombre: { $first: "$name" },
          ciudad: { 
            $first: { 
              $arrayElemAt: [{ $split: ["$address", ","] }, 1] 
            } 
          },
          total_transacciones: { $sum: 1 },
          monto_total: { $sum: { $toDouble: "$account_transactions.transactions.amount" } }
        }
      },
      {
        $addFields: {
          monto_promedio: { 
            $round: [{ $divide: ["$monto_total", "$total_transacciones"] }, 2]
          }
        }
      },
      {
        $project: {
          _id: 0,
          nombre: 1,
          ciudad: { $trim: { input: "$ciudad" } },
          total_transacciones: 1,
          monto_promedio: 1
        }
      },
      { $sort: { total_transacciones: -1 } },
      { $limit: 5 }
    ]).toArray();
    console.log(JSON.stringify(query21, null, 2));

    // 2.2 Clasificación de clientes por balance total
    console.log("\n 2.2 Clasificación de clientes por balance total:");
    const query22 = await customers.aggregate([
      { $unwind: "$accounts" },
      {
        $lookup: {
          from: "accounts",
          localField: "accounts",
          foreignField: "account_id",
          as: "account_info"
        }
      },
      { $unwind: "$account_info" },
      {
        $group: {
          _id: "$_id",
          nombre: { $first: "$name" },
          balance_total: { $sum: "$account_info.limit" }
        }
      },
      {
        $addFields: {
          categoria: {
            $switch: {
              branches: [
                { case: { $lt: ["$balance_total", 5000] }, then: "Bajo (<5,000)" },
                { case: { $and: [
                  { $gte: ["$balance_total", 5000] },
                  { $lte: ["$balance_total", 20000] }
                ]}, then: "Medio (5,000-20,000)" },
                { case: { $gt: ["$balance_total", 20000] }, then: "Alto (>20,000)" }
              ],
              default: "Sin clasificar"
            }
          }
        }
      },
      {
        $project: {
          _id: 0,
          nombre: 1,
          categoria: 1
        }
      },
      { $sort: { nombre: 1 } },
      { $limit: 5 }
    ]).toArray();
    console.log(JSON.stringify(query22, null, 2));

    // 2.3 Cliente con mayor balance por ciudad
    console.log("\n 2.3 Cliente con mayor balance por ciudad:");
    const query23 = await customers.aggregate([
      {
        $addFields: {
          ciudad: { 
            $trim: { 
              input: { 
                $arrayElemAt: [{ $split: ["$address", ","] }, 1] 
              } 
            }
          }
        }
      },
      { $unwind: "$accounts" },
      {
        $lookup: {
          from: "accounts",
          localField: "accounts",
          foreignField: "account_id",
          as: "account_info"
        }
      },
      { $unwind: "$account_info" },
      {
        $group: {
          _id: "$_id",
          nombre: { $first: "$name" },
          ciudad: { $first: "$ciudad" },
          balance_total: { $sum: "$account_info.limit" }
        }
      },
      { $sort: { ciudad: 1, balance_total: -1 } },
      {
        $group: {
          _id: "$ciudad",
          nombre: { $first: "$nombre" },
          ciudad: { $first: "$ciudad" },
          total: { $first: "$balance_total" }
        }
      },
      {
        $project: {
          _id: 0,
          nombre: 1,
          ciudad: 1,
          total: 1
        }
      },
      { $sort: { ciudad: 1 } },
      { $limit: 5 } //Eliminar para ver todas las ciudades (eran como 30 y la terminal no mostraba todo)
    ]).toArray();
    console.log(JSON.stringify(query23, null, 2));

    // 2.4 Top 10 transacciones más altas en últimos 6 meses de 2017
    console.log("\n 2.4 Top 10 transacciones más altas (últimos 6 meses):");
    const fechaLimite = new Date();
    fechaLimite.setMonth(fechaLimite.getMonth() - 6);
    fechaLimite.setFullYear(fechaLimite.getMonth() - 9);
    
    const query24 = await transactions.aggregate([
      { $unwind: "$transactions" },
      {
        $match: {
          "transactions.date": { $gte: fechaLimite }
        }
      },
      { $sort: { "transactions.amount": -1 } },
      { $limit: 10 },
      {
        $lookup: {
          from: "accounts",
          localField: "account_id",
          foreignField: "account_id",
          as: "account_info"
        }
      },
      { $unwind: "$account_info" },
      {
        $lookup: {
          from: "customers",
          localField: "account_id",
          foreignField: "accounts",
          as: "customer_info"
        }
      },
      { $unwind: "$customer_info" },
      {
        $addFields: {
          "customer_info.ciudad": {
            $trim: {
              input: {
                $arrayElemAt: [{ $split: ["$customer_info.address", ","] }, 1]}}}}},
      {
        $project: {
          _id: 0,
          monto: { $round: ["$transactions.amount", 2] },
          fecha: "$transactions.date",
          tipo: "$transactions.transaction_code",
          simbolo: "$transactions.symbol",
          cuenta: "$account_id",
          cliente_nombre: "$customer_info.name",
          cliente_ciudad: "$customer_info.ciudad",
          cliente_email: "$customer_info.email"}}
    ]).toArray();
    console.log(JSON.stringify(query24, null, 2));

    console.log("\n2.5 Variación porcentual:");
    const query25 = await customers.aggregate([
      { $unwind: "$accounts" },
      {
        $lookup: {
          from: "transactions",
          localField: "accounts",
          foreignField: "account_id",
          as: "account_transactions"
        }
      },
      { $unwind: "$account_transactions" },
      { $unwind: "$account_transactions.transactions" },
      {
        $group: {
          _id: "$_id",
          nombre: { $first: "$name" },
          transacciones: { 
            $push: {
              amount: { $toDouble: "$account_transactions.transactions.amount" },
              date: "$account_transactions.transactions.date"}}}},
      {
        $match: {
          $expr: { $gte: [{ $size: "$transacciones" }, 2] }
        }
      },
      {
        $addFields: {
          transaccion_mas_reciente: {
            $arrayElemAt: [
              {
                $sortArray: {
                  input: "$transacciones",
                  sortBy: { date: -1 }}},0]},
          transaccion_mas_antigua: {
            $arrayElemAt: [
              {
                $sortArray: {
                  input: "$transacciones",
                  sortBy: { date: 1 }}},0]}}},
      {
        $addFields: {
          variacion_porcentual: {
            $round: [
              {
                $multiply: [
                  {
                    $divide: [
                      { $subtract: ["$transaccion_mas_reciente.amount", "$transaccion_mas_antigua.amount"] },
                      "$transaccion_mas_antigua.amount"]},100]},2]}}},
      {
        $project: {
          _id: 0,
          nombre: 1,
          variacion_porcentual: 1,
          monto_antiguo: "$transaccion_mas_antigua.amount",
          monto_reciente: "$transaccion_mas_reciente.amount"
        }
      },
      { $sort: { variacion_porcentual: -1 } },
      { $limit: 5 }
    ]).toArray();
    console.log(JSON.stringify(query25, null, 2));

    // 2.6 Agrupar transacciones por mes y tipo
    console.log("\n 2.6 Transacciones por mes y tipo:");
    const query26 = await transactions.aggregate([
      { $unwind: "$transactions" },
      {
        $group: {
          _id: {
            mes: { $month: "$transactions.date" },
            año: { $year: "$transactions.date" },
            tipo: "$transactions.transaction_code"
          },
          total_transacciones: { $sum: 1 },
          monto_total: { 
            $sum: { $toDouble: "$transactions.amount" }
          },
          monto_promedio: { 
            $avg: { $toDouble: "$transactions.amount" }
          }
        }
      },
      {
        $project: {
          _id: 0,
          mes: "$_id.mes",
          año: "$_id.año",
          tipo: "$_id.tipo",
          total_transacciones: 1,
          monto_total: { $round: ["$monto_total", 2] },
          monto_promedio: { $round: ["$monto_promedio", 2] }
        }
      },
      { $sort: { año: -1, mes: -1, tipo: 1 } },
      { $limit: 10 }
    ]).toArray();
    console.log(JSON.stringify(query26, null, 2));




    console.log("\n 2.7 Clientes sin transacciones");

    const clientesConTransacciones = await transactions.aggregate([
      { $unwind: "$transactions" },
      { $group: { _id: "$account_id" } }
    ]).toArray();
    
    const accountIdsConTransacciones = clientesConTransacciones.map(c => c._id);
    const clientesInactivos = await customers.aggregate([
      {
        $match: {
          accounts: { $not: { $elemMatch: { $in: accountIdsConTransacciones } } }
        }
      },
      {
        $project: {
          _id: 1,
          nombre: "$name",
          email: 1,
          cuentas: "$accounts"
        }
      }
    ]).toArray();
    
    if (clientesInactivos.length > 0) {
      const inactiveCollection = database.collection("inactive_customers");
      await inactiveCollection.deleteMany({}); // Limpiar coleccion
      await inactiveCollection.insertMany(clientesInactivos);
      console.log(`Se guardaron ${clientesInactivos.length} clientes inactivos`);
    } else {
      console.log("No se encontraron clientes inactivos");
    }
    
    console.log("Muestra de clientes inactivos:", 
      JSON.stringify(clientesInactivos.slice(0, 3), null, 2));

    console.log("\n2.8 Resumen por tipo de cuenta (guardado en account_summaries):");
    const query28 = await accounts.aggregate([
      { $unwind: "$products" },
      {
        $group: {
          _id: "$products",
          total_cuentas: { $sum: 1 },
          balance_promedio: { $avg: "$limit" },
          balance_total: { $sum: "$limit" }
        }},
      {
        $project: {
          _id: 0,
          tipo_cuenta: "$_id",
          total_cuentas: 1,
          balance_promedio: { $round: ["$balance_promedio", 2] },
          balance_total: 1
        }}]).toArray();
    

    const summariesCollection = database.collection("account_summaries");
    await summariesCollection.deleteMany({});
    await summariesCollection.insertMany(query28);
    console.log(" Resumen guardado en account_summaries");
    console.log(JSON.stringify(query28, null, 2));

    // 2.9 Clientes de alto valor (balance > 30,000 y más de 5 transacciones)
    console.log("\n2.9 Clientes de alto valor:");
    const query29 = await customers.aggregate([
      { $unwind: "$accounts" },
      {
        $lookup: {
          from: "accounts",
          localField: "accounts",
          foreignField: "account_id",
          as: "account_info"
        }
      },
      { $unwind: "$account_info" },
      {
        $group: {
          _id: "$_id",
          nombre: { $first: "$name" },
          email: { $first: "$email" },
          balance_total: { $sum: "$account_info.limit" }
        }
      },
      {
        $match: {
          balance_total: { $gt: 30000 }
        }
      },
      {
        $lookup: {
          from: "transactions",
          localField: "_id",
          foreignField: "account_id",
          as: "transacciones"
        }
      },
      {
        $addFields: {
          total_transacciones: {
            $size: {
              $reduce: {
                input: "$transacciones",
                initialValue: [],
                in: { $concatArrays: ["$$value", "$$this.transactions"] }
              }
            }
          }
        }
      },
      {
        $match: {
          total_transacciones: { $gt: 5 }
        }
      },
      {
        $project: {
          _id: 0,
          nombre: 1,
          email: 1,
          balance_total: 1,
          total_transacciones: 1
        }
      }
    ]).toArray();
    
    if (query29.length > 0) {
      const highValueCollection = database.collection("high_value_customers");
      await highValueCollection.deleteMany({});
      await highValueCollection.insertMany(query29);
      console.log(`Se guardaron ${query29.length} clientes de alto valor`);
    }
    else{
        console.log('No hay clientes de alto valor :c')
    }
    console.log(JSON.stringify(query29, null, 2));

    // 2.10 Promedio mensual de transacciones y clasificación
    console.log("\n2.10 Promedio mensual y clasificación de clientes:");
    const fechaLimiteAnual = new Date();
    fechaLimiteAnual.setFullYear(fechaLimiteAnual.getFullYear() - 10); //2017
    
    const query210 = await customers.aggregate([
      { $unwind: "$accounts" },
      {
        $lookup: {
          from: "transactions",
          localField: "accounts",
          foreignField: "account_id",
          as: "account_transactions"
        }
      },
      { $unwind: "$account_transactions" },
      { $unwind: "$account_transactions.transactions" },
      {
        $match: {
          "account_transactions.transactions.date": { $gte: fechaLimiteAnual }
        }
      },
      {
        $group: {
          _id: "$_id",
          nombre: { $first: "$name" },
          transacciones_anuales: { $sum: 1 },
          meses_con_transacciones: {
            $addToSet: {
              $dateToString: {
                format: "%Y-%m",
                date: "$account_transactions.transactions.date"}}}}},
      {
        $addFields: {
          promedio_mensual: {
            $round: [
              { $divide: ["$transacciones_anuales", { $size: "$meses_con_transacciones" }] },1]}}},
      {
        $addFields: {
          categoria: {
            $switch: {
              branches: [
                { case: { $lt: ["$promedio_mensual", 2] }, then: "infrequent" },
                { case: { $and: [
                  { $gte: ["$promedio_mensual", 2] },
                  { $lte: ["$promedio_mensual", 5] }
                ]}, then: "regular" },
                { case: { $gt: ["$promedio_mensual", 5] }, then: "frequent" }
              ],
              default: "sin datos"}}}},
      {
        $project: {
          _id: 0,
          nombre: 1,
          promedio_mensual: 1,
          categoria: 1
        }
      },
      { $sort: { promedio_mensual: -1 } },
      { $limit: 10 }
    ]).toArray();
    console.log(JSON.stringify(query210, null, 2));


    console.log("ENDING");

  } catch (error) {
    console.error("Error:", error);
  } finally {
    await client.close();
  }
}
query();