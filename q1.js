db = db.getSiblingDB("project2");

db.main_dataset.aggregate([
{
    $match:{
        energy:{$gt: 0.5},
        duration_ms:{$gt: 90000}
    }
},
{
    $project:{
        year: {$toInt:{$substr:["$release_date", 0, 4]}},
        track_uri: 1,
        artist_info:{
            $zip: {
            inputs: ["$artists_names", "$artists_uris"]}}    
    }
},
{
    $match:{
        year:{
            $gte:2013,
            $lte:2023}
    }
},
{
    $unwind:"$artist_info"
},
{
    $group: {
        _id: {
            year:"$year",
            artist_info:"$artist_info"
        },
        total_track:{$sum: 1}
    }
},
{
    $sort: {
      "_id.year":1,
      total_track: -1
    }
},
{
    $group: {
      _id: "$_id.year",
      secondValue: { $push: "$_id.artist_info" }
    }
},
{
    $project: {
      secondValue: { $slice: ["$secondValue", 1, 1] }, // Extract the second value from the array
      _id: 1
    }
},
{
    $unwind: "$secondValue"
},
{
    $lookup: {
      from: "artists",
      localField: "secondValue.1",
      foreignField: "artist_uri",
      as: "artist_info"
    }
},
{
    $unwind: "$artist_info"
},
{
    $project:{
        artist_name: {$arrayElemAt: ["$secondValue", 0]},
        popularity: "$artist_info.artist_popularity",
        followers: "$artist_info.artist_followers"
    }    
},
{
    $sort: {
      _id:1,
    }
}
]);