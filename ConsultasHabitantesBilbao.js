// CONSULTA 1: Contar el número total de habitantes por distritos y devolverlos en órden descendente
db.HabitantesBilbao.aggregate([
   {$group: {
            _id: "$DISTRITO",
            total_habitantes: {
                $sum: {
                    $add: ["$0 ANO", "$1 ANO", "$2 ANOS", "$3 ANOS", "$4 ANOS", "$5 ANOS", "$6 ANOS", "$7 ANOS", "$8 ANOS", "$9 ANOS", "$10 ANOS", "$11 ANOS", "$12 ANOS", "$13 ANOS", "$14 ANOS", "$15 ANOS", "$16 ANOS", "$17 ANOS", "$18 ANOS", "$19 ANOS", "$20 ANOS", "$21 ANOS", "$22 ANOS", "$23 ANOS", "$24 ANOS", "$25 ANOS", "$26 ANOS", "$27 ANOS", "$28 ANOS", "$29 ANOS", "$30 ANOS", "$31 ANOS", "$32 ANOS", "$33 ANOS", "$34 ANOS", "$35 ANOS", "$36 ANOS", "$37 ANOS", "$38 ANOS", "$39 ANOS", "$40 ANOS", "$41 ANOS", "$42 ANOS", "$43 ANOS", "$44 ANOS", "$45 ANOS", "$46 ANOS", "$47 ANOS", "$48 ANOS", "$49 ANOS", "$50 ANOS", "$51 ANOS", "$52 ANOS", "$53 ANOS", "$54 ANOS", "$55 ANOS", "$56 ANOS", "$57 ANOS", "$58 ANOS", "$59 ANOS", "$60 ANOS", "$61 ANOS", "$62 ANOS", "$63 ANOS", "$64 ANOS", "$65 ANOS", "$66 ANOS", "$67 ANOS", "$68 ANOS", "$69 ANOS", "$70 ANOS", "$71 ANOS", "$72 ANOS", "$73 ANOS", "$74 ANOS", "$75 ANOS", "$76 ANOS", "$77 ANOS", "$78 ANOS", "$79 ANOS", "$80 ANOS", "$81 ANOS", "$82 ANOS", "$83 ANOS", "$84 ANOS", "$85 ANOS", "$86 ANOS", "$87 ANOS", "$88 ANOS", "$89 ANOS", "$90 ANOS", "$91 ANOS", "$92 ANOS", "$93 ANOS", "$94 ANOS", "$95 ANOS", "$96 ANOS", "$97 ANOS", "$98 ANOS", "$99 ANOS", "$100 ANOS", "$101 ANOS", "$102 ANOS", "$103 ANOS", "$104 ANOS", "$105 ANOS", "$106 ANOS", "$107 ANOS", "$108 ANOS", "$109 ANOS", "$110 ANOS", "$110 MAS"]
                }
            }
        }
    },
     {$sort: {total_habitantes:-1}}
])

// CONSULTA 2: Contar la cantidad de habitantes por distrito y sexo. Luego exportar los valores ordenados ascendentemente a un nuevo archivo llamado “habitantes_agregados.csv” 
db.HabitantesBilbao.aggregate([
  {
    $group: {
      _id: {
        DISTRITO: "$DISTRITO",
        SEXO: "$SEXO"
      },
      total_habitantes: { $sum: { $add: ["$0 ANO", "$1 ANO", "$2 ANOS", "$3 ANOS", "$4 ANOS", "$5 ANOS", "$6 ANOS", "$7 ANOS", "$8 ANOS", "$9 ANOS", "$10 ANOS", "$11 ANOS", "$12 ANOS", "$13 ANOS", "$14 ANOS", "$15 ANOS", "$16 ANOS", "$17 ANOS", "$18 ANOS", "$19 ANOS", "$20 ANOS", "$21 ANOS", "$22 ANOS", "$23 ANOS", "$24 ANOS", "$25 ANOS", "$26 ANOS", "$27 ANOS", "$28 ANOS", "$29 ANOS", "$30 ANOS", "$31 ANOS", "$32 ANOS", "$33 ANOS", "$34 ANOS", "$35 ANOS", "$36 ANOS", "$37 ANOS", "$38 ANOS", "$39 ANOS", "$40 ANOS", "$41 ANOS", "$42 ANOS", "$43 ANOS", "$44 ANOS", "$45 ANOS", "$46 ANOS", "$47 ANOS", "$48 ANOS", "$49 ANOS", "$50 ANOS", "$51 ANOS", "$52 ANOS", "$53 ANOS", "$54 ANOS", "$55 ANOS", "$56 ANOS", "$57 ANOS", "$58 ANOS", "$59 ANOS", "$60 ANOS", "$61 ANOS", "$62 ANOS", "$63 ANOS", "$64 ANOS", "$65 ANOS", "$66 ANOS", "$67 ANOS", "$68 ANOS", "$69 ANOS", "$70 ANOS", "$71 ANOS", "$72 ANOS", "$73 ANOS", "$74 ANOS", "$75 ANOS", "$76 ANOS", "$77 ANOS", "$78 ANOS", "$79 ANOS", "$80 ANOS", "$81 ANOS", "$82 ANOS", "$83 ANOS", "$84 ANOS", "$85 ANOS", "$86 ANOS", "$87 ANOS", "$88 ANOS", "$89 ANOS", "$90 ANOS", "$91 ANOS", "$92 ANOS", "$93 ANOS", "$94 ANOS", "$95 ANOS", "$96 ANOS", "$97 ANOS", "$98 ANOS", "$99 ANOS", "$100 ANOS", "$101 ANOS", "$102 ANOS", "$103 ANOS", "$104 ANOS", "$105 ANOS", "$106 ANOS", "$107 ANOS", "$108 ANOS", "$109 ANOS", "$110 ANOS", "$110 MAS"] } }
    }
  },
  {
    $project: {
      _id: 0,
      DISTRITO: "$_id.DISTRITO",
      SEXO: "$_id.SEXO",
      total_habitantes: 1
    }
  },
  {
    $sort: { total_habitantes: 1 } 
  },
  {
    $out: "HabitantesAgregados"
  }
])
  

// CONSULTA 3: Encontrar los habitantes de un distrito específico, desglosar los datos por sección y limitar el número de secciones a 5
db.HabitantesBilbao.aggregate([
    {
      $match: {
        DISTRITO: "DEUSTU"
      }
    },
    { $unwind: "$SECCION" },
    { $limit: 5 }
  ])

// CONSULTA 4: Encontrar para hombres y mujeres el distrito y sección con la mayor cantidad de habitantes de 110 años
//i) Usando operador bottom y ordenando de manera ascendente 
db.HabitantesBilbao.aggregate([
  {
    $group: {
      _id: "$SEXO",
      distrito: {
        $bottom: {
          output: ["$DISTRITO", "$SECCION", "$110 ANOS"],
          sortBy: { "110 ANOS": 1 }
        }
      }
    }
  },
  {
    $project: {
      _id: 0,
      sexo: "$_id", 
      "Cantidad máxima de habitantes": { $arrayElemAt: ["$distrito", 2] }, 
      "Sección": { $arrayElemAt: ["$distrito", 1] }, 
      "Distrito": { $arrayElemAt: ["$distrito", 0] }
    }
  }
])

//ii) Usando operador top y ordenando de manera descendente
db.HabitantesBilbao.aggregate([
  {
    $group: {
      _id: "$SEXO",
      distrito: {
        $top: {
          output: ["$DISTRITO", "$SECCION", "$110 ANOS"],
          sortBy: { "110 ANOS": -1 }
        }
      }
    }
  },
  {
    $project: {
      _id: 0,
      sexo: "$_id", 
      "Cantidad máxima de habitantes": { $arrayElemAt: ["$distrito", 2] }, 
      "Sección": { $arrayElemAt: ["$distrito", 1] }, 
      "Distrito": { $arrayElemAt: ["$distrito", 0] }
    }
  }
])

// CONSULTA 5: Agrupando por sexo en la base de datos HabitantesAgregados devolver:
//i) los primero 3 distritos y su cantidad total de habitantes
db.HabitantesAgregados.aggregate([
  {$group:
     {
        _id: "$SEXO",
        firstDistricts:
           {
              $firstN:
                 {
                    input: ["$DISTRITO","$total_habitantes"],
                    n: 3
                 }
           }
     }
  }
])

// ii) los últimos 3 distritos y su cantidad total de habitantes
db.HabitantesAgregados.aggregate([
  {$group:
     {
        _id: "$SEXO",
        firstDistricts:
           {
              $lastN:
                 {
                    input: ["$DISTRITO","$total_habitantes"],
                    n: 3
                 }
           }
     }
  }
] )

// COSNULTA 6:  Encontrar para hombres y mujeres los n distritos y secciones:
//i) con la mayor cantidad de habitantes recien nacidos
db.HabitantesBilbao.aggregate([
  {
    $group: {
      _id: "$SEXO",
      maxNDistricts: {
        $maxN: {
          input: ["$0 ANO", "$DISTRITO", "$SECCION"],
          n: 5
        }
      }
    }
  },
  {
    $project: {
      _id: 0,
      Sexo: "$_id",
      maxNDistricts: {
        $map: {
          input: "$maxNDistricts",
          as: "district",
          in: {
            Distrito: { $arrayElemAt: ["$$district", 1] },
            Seccion: { $arrayElemAt: ["$$district", 2] },
            "Cantidad de habitantes": { $arrayElemAt: ["$$district", 0] }
          }
        }
      }
    }
  }
])

//ii) con la menor cantidad de habitantes recien nacidos
db.HabitantesBilbao.aggregate([
  {
    $group: {
      _id: "$SEXO",
      minNDistricts: {
        $minN: {
          input: ["$0 ANO", "$DISTRITO", "$SECCION"],
          n: 5
        }
      }
    }
  },
  {
    $project: {
      _id: 0,
      Sexo: "$_id",
      minNDistricts: {
        $map: {
          input: "$minNDistricts",
          as: "district",
          in: {
            Distrito: { $arrayElemAt: ["$$district", 1] },
            Seccion: { $arrayElemAt: ["$$district", 2] },
            "Cantidad de habitantes": { $arrayElemAt: ["$$district", 0] }
          }
        }
      }
    }
  }
])
  