db.runCommand({ mapReduce: "restaurants",
    map : function Map() {
        var key = this.address.zipcode;
        emit(key, {
            "data":
            [
                {
                    "nombre" : this.name,
                    "calle" : this.address.street,
                    "puerta" : this.address.building,
                    "lat" : this.address.coord[0],
                    "lon" : this.address.coord[1]
                }
            ]
        });
},
 reduce : function Reduce(key, values) {
    var reduced = {"data":[]};
    for (var i in values) {
        var inter = values[i];
        for (var j in inter.data) {
            reduced.data.push(inter.data[j]);
        }
    }
    return reduced;
},
 finalize : function Finalize(key, reduced) {
    if (reduced.data.length == 1) {
        return { "message" : "Solo hay un restaurante" };
    }
    var min_dist = 999999999999;
    var restaurante1 = { "name": "" };
    var restaurante2 = { "name": "" };
    var r1;
    var r2;
    var d;
    for (var i in reduced.data) {
        for (var j in reduced.data) {
            if (i>=j) continue;
            r1 = reduced.data[i];
            r2 = reduced.data[j];
            d = (r1.lat-r2.lat)*(r1.lat-r2.lat)+(r1.lon-r2.lon)*(r1.lon-r2.lon);
            if (d < min_dist) {
                min_dist = d;
                restaurante1 = r1;
                restaurante2 = r2;
            }
        }
    }
    return {"restaurante1": restaurante1.nombre, "restaurante2": restaurante2.nombre, "dist": Math.sqrt(min_dist),"CantidadEvaluados":reduced.data.length};
},
query: {"grades.score":{$gt: 5}},
 out: "rest_mapreduce" 
 });
                    