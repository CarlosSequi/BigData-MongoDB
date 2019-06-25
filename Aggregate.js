db.runCommand({
aggregate: "restaurants",
pipeline : [
   {$match: {"grades.score" : {$gt:5}}}, //Solo considero los registros que tengan score mayor que 5
    //Agrupa por zipcode y le añade los arrays rest1 y rest2 con los datos de los restaurantes de ese tipo de zipcode
   {$group: {_id: "$address.zipcode", "rest1":{$push: {resID: "$restaurant_id", nombre:"$name", dir:"$address"}}, //, lat:"$Latitude",lon:"$Longitude"}},
                                "rest2":{$push: {resID: "$restaurant_id", nombre:"$name", dir:"$address"}},
                            "count": { $sum: 1 }}},
   {$unwind: "$rest1"}, //Desanida rest1, crea un documento por cada elemento del array rest1
   {$unwind: "$rest2"}, //Desanida rest2 crea un documento por cada elemento del array rest2
    //Calcula la distancia entre cada par de restaurantes en el campo “distancia”, devuelve otros datos necesarios.
   {$project: {_id: 0, zipcode: "$_id", rest1: "$rest1", rest2: "$rest2", count: "$count",
         distancia:{ $sqrt: {$sum: [{$pow: [{$subtract: [{$arrayElemAt: ["$rest1.dir.coord",0]},{$arrayElemAt: ["$rest2.dir.coord",0]}]},2 ]},
                                    {$pow: [{$subtract: [{$arrayElemAt: ["$rest1.dir.coord",-1]},{$arrayElemAt: ["$rest2.dir.coord",-1]}]},2 ]}]}}}},
      // Eliminamos parejas de ciudades redundantes y aquellas parejas que están a distancia 0.
   {$redact: {"$cond": [{$and:[{"$lt": ["$rest1.resID", "$rest2.resID"]}]},"$$KEEP","$$PRUNE"]}},
   {$group: {_id: "$zipcode", "dist_min": {$min: "$distancia"}, // Obtenemos las distancia mínima para cada zipcode
                            // Añadimos a la salida un “array” con los datos de todas las parejas de restaurantes de cada zipcode
                           "parejas":{$push: {rest1: "$rest1", rest2: "$rest2", distancia: "$distancia"}}, "count": {$first:"$count"}}},
   {$unwind: "$parejas"}, // Desanidamos el “array” parejas
    // Nos quedamos con aquellas parejas cuya distancia coincide con la distancia mínima de ese zipcode
   {$redact: {"$cond": [{"$eq": ["$dist_min", "$parejas.distancia"]}, "$$KEEP", "$$PRUNE"]}},
   // Proyectamos sobre los datos solicitados
   {$project: {_id: 0, "NumCodigoZip": "$_id", "Restaurante1": "$parejas.rest1.nombre",
       "Direccion1":"$parejas.rest1.dir", "Restaurante2": "$parejas.rest2.nombre",
       "Direccion2":"$parejas.rest2.dir", "distancia": "$dist_min", "count": "$count"}},
   {
			$group: {
				_id: "$NumCodigoZip",
				"distancia": { $first: "$distancia" },
				// Añadimos a la salida un “array” con los datos de todas las parejas de restaurantes
				"parejas": { $push: { rest1: "$Restaurante1", dir1: "$Direccion1", rest2: "$Restaurante2", dir2: "$Direccion2" } },
				"count": { $first: "$count" }
			}
		},
   { $out : "rest_aggregate" }
  ],
 cursor: { batchSize: 20000} ,
 allowDiskUse: true} // Permite el uso de disco para operaciones intermedias que no quepan en memoria
); 