db = db.getSiblingDB("dbms");

db.final_tracks.aggregate([
  {
      $match:{ "popularity" : { $gte : NumberInt(50) } }
  },
  {
    $unwind: "$artists_uris"
  },
  {
    $group: {
      _id: "$artists_uris",
      single_sum: {
        $sum: {
          $cond: [{ $eq: ["$album_type", "single"] }, 1, 0]
        }
      },
      album_sum: {
        $sum: {
          $cond: [{ $eq: ["$album_type", "album"] }, 1, 0]
        }
      }
    }
  },
  {
    $match: { $expr: { $gt: ["$single_sum", "$album_sum"] } }
  }
]);
