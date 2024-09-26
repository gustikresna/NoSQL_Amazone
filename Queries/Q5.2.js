db.getCollection('Customer').aggregate(
  [
    { $unwind: { path: '$currentOrder' } },
    {
      $unwind: {
        path: '$currentOrder.OrderDetails'
      }
    },
    {
      $group: {
        _id: '$currentOrder.OrderDetails.productId',
        totalQuantity: {
          $sum: '$currentOrder.OrderDetails.qty'
        }
      }
    },
    {
      $lookup: {
        from: 'OtherProduct',
        localField: '_id',
        foreignField: '_id',
        as: 'product_docs'
      }
    },
    { $unwind: { path: '$product_docs' } },
    {
      $group: {
        _id: '$product_docs.category',
        totalRevenue: {
          $sum: {
            $multiply: [
              '$product_docs.price',
              '$totalQuantity'
            ]
          }
        }
      }
    }
  ],
  { maxTimeMS: 60000, allowDiskUse: true }
);