db = db.getSiblingDB("dbms");

db.getCollection("main_dataset").aggregate([
  {
    $match: {
      danceability: { $gt: 0.6 },
      valence: { $gt: 0.6 },
      tempo: { $gt: 100 },
      mode: 1,
      loudness: { $gt: -4 }
    }
  },
  {
    $project: {
      year: { $toInt: { $substr: ["$release_date", 0, 4] } },
      popularity: 1
    }
  },
  {
    $group: {
      _id: "$year",
      averagePopularity: { $avg: "$popularity" }
    }
  },
  {
    $sort: {
      averagePopularity: -1
    }
  },
    {
    $limit: 5
  }
]);