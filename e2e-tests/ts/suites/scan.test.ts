import { assert, test } from 'vitest';
import { ReadCommand }  from '../commands/ReadCommand';

test('limit 1', async () => {
  assert.deepEqual(
      await new ReadCommand()
          .limit(1)
          .execute()
          .object(),
      {
        meta: {
          requestType: 'Scan',
          consumedCapacity: 128.5,
          requestCount: 1,
          scannedCount: 7176,
          hitCount: 7176
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

test('scan limit 1', async () => {
  assert.deepEqual(
      await new ReadCommand()
          .scanLimit(1)
          .execute()
          .object(),
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
  assert.strictEqual(
      await new ReadCommand()
          .scanLimit(1)
          .consistentRead()
          .execute()
          .object()
          .then(({ meta }) => meta.consumedCapacity),
      1
  );
});

test('scan limit 5 -> 5 items', async () => {
  assert.lengthOf(
      await new ReadCommand()
          .scanLimit(5)
          .consistentRead()
          .execute()
          .object()
          .then(({ content }) => content),
      5
  );
});

test('where last name is "Lollobrigida", limit 1, concurrency 6', async () => {
  assert.deepEqual(
      await new ReadCommand()
          .where('.last_name == "Lollobrigida"')
          .limit(1)
          .concurrency(6)
          .execute()
          .object(),
      {
        meta: {
          requestType: 'Scan',
          consumedCapacity: 462.5,
          requestCount: 6,
          scannedCount: 20948,
          hitCount: 1
        },
        content: [
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
      }
  );
});
