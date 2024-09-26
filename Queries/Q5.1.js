db.getCollection('FreshProduct').aggregate(
  [
    { $match: { rating: { $gt: 4 } } },
    { $sort: { price: -1 } },
    { $limit: 10 }
  ],
  { maxTimeMS: 60000, allowDiskUse: true }
);