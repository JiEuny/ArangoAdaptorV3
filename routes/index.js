var express = require('express');
var router = express.Router();

var arangojs = require("arangojs");
const db = new arangojs.Database();
const aql = arangojs.aql;
const fs = require('fs');

db.useDatabase("_system");
db.useBasicAuth("root", "0000");

const entityColl = db.collection('entityColl');
const entityEdge = db.edgeCollection('entityEdge');
const propertyColl = db.collection('propertyColl');
const propertyEdge = db.edgeCollection('propertyEdge');

collectionSetup();

async function collectionSetup() {
  const entityCollExist = await entityColl.exists();
  const entityEdgeExist = await entityEdge.exists();
  const propertyCollExist = await propertyColl.exists();
  const propertyEdgeExist = await propertyEdge.exists();

  if (entityCollExist != true) {
    entityColl.create();
  }

  if (entityEdgeExist != true) {
    entityEdge.create();
  }

  if (propertyCollExist != true) {
    propertyColl.create();
  }

  if (propertyEdgeExist != true) {
    propertyEdge.create();
  }

}

let rawdata = fs.readFileSync('./ngsiDataset/infectionCase.json');
let cases = JSON.parse(rawdata);

function storeEntity(bodies) {
  // console.log(bodies);
  let properties = new Array();
  let entities = new Array();
  let relationships = new Array();
  for (const body of bodies) {
    // console.log(body);
    var property = [{
      id: 'entityColl/' + body.id
    }];
    var entity = [{
      _key: body.id
    }];
    var relationship = [{
      id: 'entityColl/' + body.id
    }];

    for (const [key, value] of Object.entries(body)) {

      // console.log(value[0]);

      if (value.type == "Property" || value.type == "GeoProperty") {
        
        property = property.map(function (d) {
          var o = Object.assign({}, d);
          o[key] = value;
          return o;
        });
        // console.log(property);
      // } else if (value.type == "Relationship" || value[0].type == "Relationship") {
      } else if (value.type == "Relationship" || key == "visitedPlace") {
        relationship = relationship.map(function (d) {
          var o = Object.assign({}, d);
          o[key] = value;
          return o;
        })
        // console.log(relationship);
      } else {

        entity = entity.map(function (d) {
          var o = Object.assign({}, d);
          o[key] = value;
          return o;
        });
      }
    }
    // console.log(relationship);
    relationships.push(relationship);
    entities.push(entity);
    properties.push(property);
  }
  // console.log(entities);
  // console.log(properties);
  for (const entity of entities) {
    // console.log(entity[0]);
    db.query(aql`
      INSERT ${entity[0]} INTO ${entityColl}
      RETURN NEW
    `).then(function (cursor) {
      // console.log(cursor._result);
    })
  }
  // console.log(properties);

  for (const property of properties) {
    storeProperty(property);
    // console.log(property);
  }

  for (const relationship of relationships) {
    // console.log(relationship);
    storeEdge(relationship);
  }
}

function storeEdge(bodies) {
  for (const body of bodies) {
    // console.log(body);
    for (const [key, value] of Object.entries(body)) {
      // console.log(key);
      if (value.type == "Relationship") {
        // console.log(key);
        // console.log(value);
        db.query(aql`
          FOR doc IN ${entityColl}
            FILTER doc.id == ${value.object}
            let toId = doc._id
          INSERT { _from: ${body.id}, _to: toId, attributeName: ${key}} INTO ${entityEdge}
          RETURN NEW
        `).then(function (cursor) {
          // console.log(cursor._result);
        })
      } else if(key != "id") {

        // console.log(value);
        for(const [key2, value2] of Object.entries(value)) {
          // console.log(value2);
          db.query(aql`
            FOR doc IN ${entityColl}
              FILTER doc.id == ${value2.object}
              let toId = doc._id
            INSERT { _from: ${body.id}, _to: toId, attributeName: ${key}} INTO ${entityEdge}
            RETURN NEW
          `).then(function (cursor) {
            console.log(cursor._result);
          })
        }
      }
    }
  }
}

function storeProperty(bodies) {
  for (const body of bodies) {
    // console.log(body);
    for (const [key, value] of Object.entries(body)) {
      // console.log(key+":"+value);
      // console.log(key);
      if (value.type == "Property" || value.type == "GeoProperty") {
        // console.log(value);
        db.query(aql`
          INSERT {
            type: ${value.type},
            value: ${value.value}
          } INTO ${propertyColl}
          let property = NEW
          INSERT { _from: ${body.id}, _to: NEW._id, attributeName: ${key}} INTO ${propertyEdge}
          RETURN property
        `).then(function (cursor) {
          // console.log(cursor._result);
          var property = [{
            id: cursor._result[0]._id
          }];
          var relationship = [{
            id: cursor._result[0]._id
          }];

          for (const [key2, value2] of Object.entries(value)) {
            // console.log(value2);
            if (value2.type == "Property" || value2.type == "GeoProperty") {
              // console.log(value2);
              property = property.map(function (d) {
                var o = Object.assign({}, d);
                o[key2] = value2;
                return o;
              });
              // console.log(property);
              storeProperty(property);

            } else if (value2.type == "Relationship") {
              relationship = relationship.map(function (d) {
                var o = Object.assign({}, d);
                o[key2] = value2;
                return o;
              })
              storeEdge(relationship);
            }
          }
        })
      }
    }
  }
}

storeEntity(cases);

/* GET home page. */
router.get('/', function (req, res, next) {
  res.render('index', { title: 'Express' });
});

module.exports = router;
