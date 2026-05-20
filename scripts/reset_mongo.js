var cols = ["youtube_raw","google_news_raw","vnexpress_raw","tuoitre_raw","reddit_raw"];
cols.forEach(function(col){
  var r = db[col].updateMany({}, {$unset:{processed_at:"",brand:""}});
  print(col + ": modified " + r.modifiedCount);
});
