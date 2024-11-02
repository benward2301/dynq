import {
  assert,
  test
}                      from 'vitest';
import { ReadCommand } from '../commands/ReadCommand.js';

test('partition key category, where name is "Comedy" -> meta, category#5', async () => {
  const output = await new ReadCommand()
      .partitionKey('.entity = "category"')
      .where('.name == "Comedy"')
      .execute()
      .parse();
  assert.deepEqual(
      output,
      {
        meta: {
          requestType: 'Query',
          consumedCapacity: 0.5,
          requestCount: 1,
          scannedCount: 16,
          hitCount: 1
        },
        content: [
          {
            category_id: 5,
            last_update: '2006-02-15T09:46:27',
            name: 'Comedy',
            id: 5,
            uuid: 'add29b12-9578-11ef-8ef0-473a18243738',
            entity: 'category'
          }
        ]
      }
  );
});

test('partition keys [actor, staff, customer], pretransform to first name, where begins with "Mi"' +
    ' -> all first names beginning with "Mi"',
    async () => {
      const { content } = await new ReadCommand()
          .partitionKey('.entity = ["actor", "staff", "customer"]')
          .pretransform('.first_name')
          .where('.[0:2] == "Mi"')
          .execute()
          .parse();
      assert.deepEqual(
          content.sort(),
          [
            'Michael',
            'Michael',
            'Michael',
            'Micheal',
            'Michele',
            'Michelle',
            'Michelle',
            'Miguel',
            'Mike',
            'Mike',
            'Mildred',
            'Milla',
            'Milla',
            'Milton',
            'Minnie',
            'Minnie',
            'Minnie',
            'Miriam',
            'Misty',
            'Mitchell'
          ]
      );
    }
);

test('partition key staff, select first and last names, content only -> staff full names', async () => {
  const content = await new ReadCommand()
      .partitionKey('.entity = "staff"')
      .select('first_name, last_name')
      .contentOnly()
      .execute()
      .parse();
  assert.sameDeepOrderedMembers(
      content,
      [
        {
          last_name: 'Hillyer',
          first_name: 'Mike'
        },
        {
          last_name: 'Stephens',
          first_name: 'Jon'
        }
      ]
  );
});

test('partition key country, limit 1, content only -> country#1', async () => {
  const content = await new ReadCommand()
      .partitionKey('.entity = "country"')
      .limit(1)
      .contentOnly()
      .execute()
      .parse();
  assert.deepEqual(
      content,
      [
        {
          country: 'Afghanistan',
          last_update: '2006-02-15T09:44:00',
          id: 1,
          uuid: '5ae8d86c-9578-11ef-be19-afd2380f219b',
          country_id: 1,
          entity: 'country'
        }
      ]
  );
});

test('partition key country, scan limit 1, start key country#2 -> meta, country#3', async () => {
  const output = await new ReadCommand()
      .partitionKey('.entity = "country"')
      .scanLimit(1)
      .startKey({
        entity: 'country',
        id: 2
      })
      .execute()
      .parse();
  assert.deepEqual(
      output,
      {
        meta: {
          requestType: 'Query',
          consumedCapacity: 0.5,
          requestCount: 1,
          scannedCount: 1,
          hitCount: 1,
          lastEvaluatedKey: {
            id: 3,
            entity: 'country'
          }
        },
        content: [
          {
            country: 'American Samoa',
            last_update: '2006-02-15T09:44:00',
            id: 3,
            uuid: '5b750652-9578-11ef-a2f7-ebcac583dc26',
            country_id: 3,
            entity: 'country'
          }
        ]
      }
  );
});

test('partition key staff, consistent read -> meta', async () => {
  const { meta } = await new ReadCommand()
      .partitionKey('.entity = "staff"')
      .consistentRead()
      .execute()
      .parse();
  assert.deepEqual(
      meta,
      {
        requestType: 'Query',
        consumedCapacity: 39,
        requestCount: 1,
        scannedCount: 2,
        hitCount: 2
      }
  );
});

test('partition key category, sort key < 4 -> first three categories', async () => {
  const { content } = await new ReadCommand()
      .partitionKey('.entity = "category"')
      .sortKey('.id.lt = 4')
      .execute()
      .parse();
  assert.deepEqual(
      content,
      [
        {
          category_id: 1,
          last_update: '2006-02-15T09:46:27',
          name: 'Action',
          id: 1,
          uuid: 'acc3cbd8-9578-11ef-a16c-9bb86fce37d9',
          entity: 'category'
        },
        {
          category_id: 2,
          last_update: '2006-02-15T09:46:27',
          name: 'Animation',
          id: 2,
          uuid: 'ad06e922-9578-11ef-af20-972ff3d3497a',
          entity: 'category'
        },
        {
          category_id: 3,
          last_update: '2006-02-15T09:46:27',
          name: 'Children',
          id: 3,
          uuid: 'ad4a66e8-9578-11ef-954a-bfbf581f5224',
          entity: 'category'
        }
      ]
  );
});

test('partition key category, 4 <= sort key <= 6 -> category#4:7', async () => {
  const { content } = await new ReadCommand()
      .partitionKey('.entity = "category"')
      .sortKey('.id = { gte: 4, lte: 6 }')
      .execute()
      .parse();
  assert.deepEqual(
      content,
      [
        {
          category_id: 4,
          last_update: '2006-02-15T09:46:27',
          name: 'Classics',
          id: 4,
          uuid: 'ad8f0bb8-9578-11ef-9ad1-6b04878eb9e5',
          entity: 'category'
        },
        {
          category_id: 5,
          last_update: '2006-02-15T09:46:27',
          name: 'Comedy',
          id: 5,
          uuid: 'add29b12-9578-11ef-8ef0-473a18243738',
          entity: 'category'
        },
        {
          category_id: 6,
          last_update: '2006-02-15T09:46:27',
          name: 'Documentary',
          id: 6,
          uuid: 'ae158b84-9578-11ef-a542-0fce1147f395',
          entity: 'category'
        }
      ]
  );
});

test('partition key category, sort key > 14 -> category#15:', async () => {
  const { content } = await new ReadCommand()
      .partitionKey('.entity = "category"')
      .sortKey('.id.gt = 14')
      .execute()
      .parse();
  assert.deepEqual(
      content,
      [
        {
          category_id: 15,
          last_update: '2006-02-15T09:46:27',
          name: 'Sports',
          id: 15,
          uuid: 'b076bc72-9578-11ef-b6db-03f91f0b91f5',
          entity: 'category'
        },
        {
          category_id: 16,
          last_update: '2006-02-15T09:46:27',
          name: 'Travel',
          id: 16,
          uuid: 'b0ba9c9e-9578-11ef-9be2-0764f0316f58',
          entity: 'category'
        }
      ]
  );
});

test('global index payment_id, partition key 32098, sort key 5c5e42d6 -> meta, payment#32098', async () => {
      const output = await new ReadCommand()
          .globalIndex('payment_id')
          .partitionKey('.payment_id = 32098')
          .sortKey('.uuid = "5c5e42d6-6652-11ef-bb18-bf8c0bb842c0"')
          .execute()
          .parse();
      assert.deepEqual(
          output,
          {
            meta: {
              requestType: 'Query',
              consumedCapacity: 0.5,
              requestCount: 1,
              scannedCount: 1,
              hitCount: 1
            },
            content: [
              {
                payment_id: 32098,
                id: 32098,
                uuid: '5c5e42d6-6652-11ef-bb18-bf8c0bb842c0',
                entity: 'payment'
              }
            ]
          }
      );
    }
);

test('global index address_id, partition key 10 -> address#10 keys, customer#6 keys', async () => {
  const { content } = await new ReadCommand()
      .globalIndex('address_id')
      .partitionKey('.address_id = 10')
      .execute()
      .parse();
  assert.sameDeepMembers(
      content,
      [
        {
          address_id: 10,
          id: 6,
          uuid: '5bed555c-65a4-11ef-9824-ffafc9f9b914',
          entity: 'customer'
        },
        {
          address_id: 10,
          id: 10,
          uuid: 'a9c678be-9577-11ef-abc7-d34ae2d17f44',
          entity: 'address'
        }
      ]
  );
});

test('global index film_id, partition key 604, sort key begins with "1" -> film_actor#1147 keys, film#604 keys', async () => {
  const { content } = await new ReadCommand()
      .globalIndex('film_id')
      .partitionKey('.film_id = 604')
      .sortKey('.uuid.beg = "1"')
      .execute()
      .parse();
  assert.deepEqual(
      content,
      [
        {
          id: 1147,
          film_id: 604,
          uuid: '18d1d19a-64cc-11ef-b55b-efba7cbc7629',
          entity: 'film_actor'
        },
        {
          id: 604,
          film_id: 604,
          uuid: '1a4476bc-64c3-11ef-b2df-5396b83cf5bb',
          entity: 'film'
        }
      ]
  );
});
