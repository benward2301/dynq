import {
  assert,
  test
}                      from 'vitest';
import { ReadCommand } from '../commands/ReadCommand.js';

test('partition key film, sort key 50, content only -> film#50', async () => {
  const content = await new ReadCommand()
      .partitionKey('.entity = "film"')
      .sortKey('.id = 50')
      .contentOnly()
      .execute()
      .parse();
  assert.deepEqual(
      content,
      [
        {
          special_features: [
            'Commentaries',
            'Behind the Scenes'
          ],
          rental_duration: 3,
          rental_rate: 2.99,
          release_year: 2006,
          length: 182,
          replacement_cost: 20.99,
          rating: 'G',
          description: 'A Stunning Drama of a Forensic Psychologist And a Husband who must Overcome a Waitress in ' +
              'A Monastery',
          language_id: 1,
          title: 'Baked Cleopatra',
          uuid: '87115ee6-64c2-11ef-9ef5-5f0c76579979',
          last_update: '2013-05-26T14:50:58.951',
          fulltext: `'bake':1 'cleopatra':2 'drama':5 'forens':8 'husband':12 'monasteri':20 'must':14 ` +
              `'overcom':15 'psychologist':9 'stun':4 'waitress':17`,
          film_id: 50,
          id: 50,
          entity: 'film'
        }
      ]
  );
});

test('partition key [store, inventory], sort key 1, concurrency 2 -> meta, store#1, inventory#1', async () => {
  const { meta, content } = await new ReadCommand()
      .partitionKey('.entity=["store", "inventory"]')
      .sortKey('.id = 1')
      .concurrency(2)
      .execute()
      .parse();
  assert.deepEqual(
      meta,
      {
        requestType: 'GetItem',
        consumedCapacity: 1,
        requestCount: 2,
        hitCount: 2
      }
  );
  assert.sameDeepMembers(
      content,
      [
        {
          store_id: 1,
          inventory_id: 1,
          last_update: '2006-02-15T10:09:17',
          film_id: 1,
          id: 1,
          uuid: 'f27a04f8-65a3-11ef-9f86-dbcc3aba1862',
          entity: 'inventory'
        },
        {
          store_id: 1,
          manager_staff_id: 1,
          last_update: '2006-02-15T09:57:12',
          address_id: 1,
          id: 1,
          uuid: 'beecd3ae-9578-11ef-8332-df0d9329a2d6',
          entity: 'store'
        }
      ]
  );
});

test('select (reserved word) name, partition key category, sort key 11, content only -> name "Horror"', async () => {
  const content = await new ReadCommand()
      .select('name')
      .partitionKey('.entity = "category"')
      .sortKey('.id = 11')
      .contentOnly()
      .execute()
      .parse();
  assert.deepEqual(
      content,
      [
        {
          name: 'Horror'
        }
      ]
  );
});