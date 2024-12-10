import {
  assert,
  test
}                      from 'vitest';
import { ReadCommand } from '../commands/ReadCommand.js';
import { TABLE_COUNT } from '../table.js';

test('limit 1 -> film_category#1', async () => {
  const { content } = await new ReadCommand()
      .limit(1)
      .execute()
      .parse();
  assert.deepEqual(
      content,
      [
        {
          category_id: 6,
          last_update: '2006-02-15T10:07:09',
          film_id: 1,
          id: 1,
          uuid: 'f4f644dc-64ca-11ef-ae4f-87ba04d99ac2',
          entity: 'film_category'
        }
      ]
  );
});

test('scan limit 1 -> meta, film_category#1', async () => {
  const output = await new ReadCommand()
      .scanLimit(1)
      .execute()
      .parse();
  assert.deepEqual(
      output,
      {
        meta: {
          requestType: 'Scan',
          consumedCapacity: 0.5,
          requestCount: 1,
          scannedCount: 1,
          hitCount: 1,
          lastEvaluatedKey: {
            id: 1,
            entity: 'film_category'
          }
        },
        content: [
          {
            category_id: 6,
            last_update: '2006-02-15T10:07:09',
            film_id: 1,
            id: 1,
            uuid: 'f4f644dc-64ca-11ef-ae4f-87ba04d99ac2',
            entity: 'film_category'
          }
        ]
      }
  );
});

test('scan limit 1, consistent read -> consumed capacity 1', async () => {
  const { meta } = await new ReadCommand()
      .scanLimit(1)
      .consistentRead()
      .execute()
      .parse();
  assert.strictEqual(meta.consumedCapacity, 1);
});

test('scan limit 5 -> 5 items', async () => {
  const { meta, content } = await new ReadCommand()
      .scanLimit(5)
      .consistentRead()
      .execute()
      .parse();
  assert.strictEqual(meta.scannedCount, 5);
  assert.strictEqual(meta.hitCount, 5);
  assert.lengthOf(content, 5);
});

test('where last name is "Lollobrigida", limit 1, concurrency 6 -> actor#5', async () => {
  const { content } = await new ReadCommand()
      .where('.last_name == "Lollobrigida"')
      .limit(1)
      .concurrency(6)
      .execute()
      .parse();
  assert.deepEqual(
      content,
      [
        {
          last_update: '2013-05-26T14:47:57.62',
          last_name: 'Lollobrigida',
          actor_id: 5,
          id: 5,
          first_name: 'Johnny',
          uuid: '7d72b57e-64c2-11ef-9b4d-7341c66f87d6',
          entity: 'actor'
        }
      ]
  );
});

test('transform to entity, unique, concurrency 6 -> all entities, whole table scanned', async () => {
  const { meta, content } = await new ReadCommand()
      .transform('.entity')
      .aggregate('unique')
      .concurrency(6)
      .execute()
      .parse();
  assert.strictEqual(meta.hitCount, TABLE_COUNT);
  assert.strictEqual(meta.scannedCount, TABLE_COUNT);
  assert.deepEqual(
      content.sort(),
      [
        'actor',
        'address',
        'category',
        'city',
        'country',
        'customer',
        'film',
        'film_actor',
        'film_category',
        'inventory',
        'language',
        'payment',
        'rental',
        'staff',
        'store'
      ]
  );
});

test('where entity is film, count, concurrency 6 -> 1000', async () => {
  const { meta, content } = await new ReadCommand()
      .where('.entity == "film"')
      .aggregate('length')
      .concurrency(6)
      .execute()
      .parse();
  assert.strictEqual(meta.hitCount, 1000);
  assert.strictEqual(content, 1000);
});

test('pretransform to full name, where "Dan Harris", concurrency 6 -> "Dan Harris"', async () => {
  const { content } = await new ReadCommand()
      .pretransform('.first_name + " " + .last_name')
      .where('. == "Dan Harris"')
      .concurrency(6)
      .execute()
      .parse();
  assert.deepEqual(content, ['Dan Harris']);
});

test('limit 1, concurrency 2 -> no last evaluated key', async () => {
  const { meta } = await new ReadCommand()
      .limit(1)
      .concurrency(2)
      .execute()
      .parse();
  assert.notProperty(meta, 'lastEvaluatedKey');
});

test('where false, scan limit 1 -> no items', async () => {
  const { meta, content } = await new ReadCommand()
      .where('false')
      .scanLimit(1)
      .execute()
      .parse();
  assert.strictEqual(meta.hitCount, 0);
  assert.deepEqual(content, []);
});

test('start key country#1, scan limit 1 -> country#2', async () => {
  const { meta, content } = await new ReadCommand()
      .startKey({
        entity: 'country',
        id: 1
      })
      .scanLimit(1)
      .execute()
      .parse();
  const lastEvaluatedKey = {
    entity: 'country',
    id: 2
  };
  assert.deepEqual(meta.lastEvaluatedKey, lastEvaluatedKey);
  assert.deepEqual(
      content[0],
      {
        ...lastEvaluatedKey,
        country: 'Algeria',
        last_update: '2006-02-15T09:44:00',
        uuid: '5b2e9de8-9578-11ef-b97c-fb53082728e2',
        country_id: 2
      }
  );
});

test('rearrange attrs, scan limit 1 -> sorted keys', async () => {
  const { content } = await new ReadCommand()
      .rearrangeAttrs()
      .scanLimit(1)
      .execute()
      .parse();
  const keys = Object.keys(content[0]);
  assert.sameOrderedMembers(keys, [...keys].sort());
});

test('where id is 928, transform to id, concurrency 6, stream, -> 5 928s', async () => {
  const output = await new ReadCommand()
      .where('.id == 928')
      .concurrency(6)
      .transform('.id')
      .stream()
      .execute()
      .raw();
  assert.deepEqual(
      output,
      new Array(5).fill(928).join('\n') + '\n'
  );
});

test('transform to uuid, sort and take 5, concurrency 3 -> meta, 5 smallest uuids', async () => {
  const output = await new ReadCommand()
      .transform('.uuid')
      .aggregate('sort | .[0:5]')
      .concurrency(3)
      .execute()
      .parse();
  assert.deepEqual(
      output,
      {
        meta: {
          requestType: 'Scan',
          consumedCapacity: 955,
          requestCount: 9,
          scannedCount: 44820,
          hitCount: 44820
        },
        content: [
          '00010736-65ac-11ef-83b8-afc605e0ae1e',
          '0001075c-64c3-11ef-ab3e-27885bd0e4c0',
          '00049bec-64cd-11ef-9a90-638859ebe0f8',
          '0005c03c-65a7-11ef-8335-133da5598935',
          '0005f03a-64cf-11ef-ac3e-a7c5fc596ae7'
        ]
      }
  );
});

test('transform to uuid, prune to smallest 5, concurrency 3 -> meta, 5 smallest uuids', async () => {
  const output = await new ReadCommand()
      .transform('.uuid')
      .prune('sort | .[0:5]')
      .concurrency(3)
      .execute()
      .parse();
  assert.deepEqual(
      output,
      {
        meta: {
          requestType: 'Scan',
          consumedCapacity: 955,
          requestCount: 9,
          scannedCount: 44820,
          hitCount: 44820
        },
        content: [
          '00010736-65ac-11ef-83b8-afc605e0ae1e',
          '0001075c-64c3-11ef-ab3e-27885bd0e4c0',
          '00049bec-64cd-11ef-9a90-638859ebe0f8',
          '0005c03c-65a7-11ef-8335-133da5598935',
          '0005f03a-64cf-11ef-ac3e-a7c5fc596ae7'
        ]
      }
  );
});

test('request limit 2, aggregate length -> meta, 11545', async () => {
  const output = await new ReadCommand()
      .requestLimit(2)
      .aggregate('length')
      .execute()
      .parse();
  assert.deepEqual(
      output,
      {
        meta: {
          requestType: 'Scan',
          consumedCapacity: 257,
          requestCount: 2,
          scannedCount: 11545,
          hitCount: 11545,
          lastEvaluatedKey: {
            id: 20365,
            entity: 'payment'
          }
        },
        content: 11545
      }
  );
});

test('request limit 1, aggregate length, concurrency 3 -> meta, 15903', async () => {
  const output = await new ReadCommand()
      .requestLimit(1)
      .aggregate('length')
      .concurrency(3)
      .execute()
      .parse();
  assert.deepEqual(
      output,
      {
        meta: {
          requestType: 'Scan',
          consumedCapacity: 365,
          requestCount: 3,
          scannedCount: 15903,
          hitCount: 15903
        },
        content: 15903
      }
  );
});

test('items per request 10, request limit 2, aggregate length -> meta, 20', async () => {
  const output = await new ReadCommand()
      .itemsPerRequest(10)
      .requestLimit(2)
      .aggregate('length')
      .execute()
      .parse();
  assert.deepEqual(
      output,
      {
        meta: {
          requestType: 'Scan',
          consumedCapacity: 1,
          requestCount: 2,
          scannedCount: 20,
          hitCount: 20,
          lastEvaluatedKey: {
            id: 20,
            entity: 'film_category'
          }
        },
        content: 20
      }
  );
});

test('scan limit 10000, aggregate length -> meta, 10000', async () => {
  const output = await new ReadCommand()
      .scanLimit(10000)
      .aggregate('length')
      .execute()
      .parse();
  assert.deepEqual(
      output,
      {
        meta: {
          requestType: 'Scan',
          consumedCapacity: 227.5,
          requestCount: 2,
          scannedCount: 10000,
          hitCount: 10000,
          lastEvaluatedKey: {
            id: 18820,
            entity: 'payment'
          }
        },
        content: 10000
      }
  );
});

test('metadata only, where id is 1, concurrency 3 -> meta', async () => {
  const output = await new ReadCommand()
      .metadataOnly()
      .where('.id == 1')
      .concurrency(3)
      .execute()
      .parse();
  assert.deepEqual(
      output,
      {
        requestType: 'Scan',
        consumedCapacity: 955,
        requestCount: 9,
        scannedCount: 44820,
        hitCount: 14
      }
  );
});
