db.getCollection('PastOrder').aggregate(
  [
    {
      $match: {
        orderDate: {
          $gte: ISODate(
            '2023-01-01T00:00:00.000Z'
          ),
          $lte: ISODate(
            '2023-12-31T23:59:59.999Z'
          )
        }
      }
    },
    {
      $group: {
        _id: '$driverId',
        totalOrders: { $sum: 1 },
        averageRating: {
          $avg: '$CustDriverRating'
        }
      }
    },
    {
      $sort: {
        totalOrders: -1,
        averageRating: -1
      }
    },
    { $match: { totalOrders: { $gte: 10 } } }
  ],
  { maxTimeMS: 60000, allowDiskUse: true }
);