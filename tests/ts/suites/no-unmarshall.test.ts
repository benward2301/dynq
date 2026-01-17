import {
  assert,
  test
}                      from 'vitest';
import { ReadCommand } from '../commands/ReadCommand.js';

test('basic scan with no-unmarshall -> type descriptors', async () => {
  const { content } = await new ReadCommand()
      .scanLimit(1)
      .noUnmarshall()
      .execute()
      .parse();
  assert.lengthOf(content, 1);
  const item = content[0];
  assert.property(item.entity, 'S');
  assert.property(item.id, 'N');
});

test('get single item with no-unmarshall -> type descriptors', async () => {
  const { content } = await new ReadCommand()
      .key('.entity = "category" | .id = 11')
      .noUnmarshall()
      .execute()
      .parse();
  assert.lengthOf(content, 1);
  assert.deepEqual(content[0].category_id, { N: '11' });
  assert.deepEqual(content[0].name, { S: 'Horror' });
  assert.deepEqual(content[0].id, { N: '11' });
  assert.deepEqual(content[0].entity, { S: 'category' });
  assert.property(content[0].uuid, 'S');
});

test('no-unmarshall with content-only -> raw content only', async () => {
  const content = await new ReadCommand()
      .key('.entity = "category" | .id = 11')
      .noUnmarshall()
      .contentOnly()
      .execute()
      .parse();
  assert.lengthOf(content, 1);
  assert.deepEqual(content[0].category_id, { N: '11' });
  assert.deepEqual(content[0].name, { S: 'Horror' });
  assert.deepEqual(content[0].id, { N: '11' });
  assert.deepEqual(content[0].entity, { S: 'category' });
  assert.property(content[0].uuid, 'S');
});

test('no-unmarshall with meta-only -> metadata only', async () => {
  const output = await new ReadCommand()
      .key('.entity = "category" | .id = 11')
      .noUnmarshall()
      .metadataOnly()
      .execute()
      .parse();
  assert.deepEqual(
      output,
      {
        consumedCapacity: 0.5,
        requestCount: 1,
        hitCount: 1
      }
  );
});

test('no-unmarshall with limit -> limited raw items', async () => {
  const { content } = await new ReadCommand()
      .partitionKey('.entity = "category"')
      .limit(2)
      .noUnmarshall()
      .execute()
      .parse();
  assert.lengthOf(content, 2);
  assert.property(content[0].name, 'S');
  assert.property(content[1].name, 'S');
});

test('no-unmarshall with stream -> streaming raw output', async () => {
  const output = await new ReadCommand()
      .key('.entity = "category" | .id = 11')
      .noUnmarshall()
      .stream()
      .execute()
      .raw();
  const parsed = JSON.parse(output.trim());
  assert.property(parsed.name, 'S');
  assert.strictEqual(parsed.name.S, 'Horror');
});

test('no-unmarshall with select (projection) -> projected raw items', async () => {
  const content = await new ReadCommand()
      .partitionKey('.entity = "category"')
      .sortKey('.id = 5')
      .select('name, id')
      .noUnmarshall()
      .contentOnly()
      .execute()
      .parse();
  assert.deepEqual(
      content,
      [
        {
          name: { S: 'Comedy' },
          id: { N: '5' }
        }
      ]
  );
});

test('no-unmarshall with concurrency -> parallel raw scan', async () => {
  const { meta, content } = await new ReadCommand()
      .where('.entity.S == "language"')
      .concurrency(3)
      .noUnmarshall()
      .execute()
      .parse();
  assert.strictEqual(meta.hitCount, 6);
  assert.lengthOf(content, 6);
  content.forEach((item: any) => {
    assert.property(item.entity, 'S');
    assert.strictEqual(item.entity.S, 'language');
  });
});

test('no-unmarshall with start-key -> pagination with raw items', async () => {
  const { meta, content } = await new ReadCommand()
      .partitionKey('.entity = "category"')
      .startKey({ id: 5 })
      .scanLimit(2)
      .noUnmarshall()
      .execute()
      .parse();
  assert.lengthOf(content, 2);
  assert.property(content[0].id, 'N');
  assert.strictEqual(content[0].id.N, '6');
  assert.property(meta, 'lastEvaluatedKey');
});

test('no-unmarshall with index -> secondary index raw query', async () => {
  const { content } = await new ReadCommand()
      .index('payment_id')
      .key('.payment_id = 32098')
      .noUnmarshall()
      .execute()
      .parse();
  assert.deepEqual(
      content,
      [
        {
          payment_id: { N: '32098' },
          id: { N: '32098' },
          uuid: { S: '5c5e42d6-6652-11ef-bb18-bf8c0bb842c0' },
          entity: { S: 'payment' }
        }
      ]
  );
});

test('no-unmarshall with where -> filter using raw structure', async () => {
  const { content } = await new ReadCommand()
      .partitionKey('.entity = "category"')
      .where('.id.N == "5"')
      .noUnmarshall()
      .execute()
      .parse();
  assert.lengthOf(content, 1);
  assert.strictEqual(content[0].name.S, 'Comedy');
});

test('no-unmarshall with transform -> transform using raw structure', async () => {
  const content = await new ReadCommand()
      .key('.entity = "category" | .id = 5')
      .transform('.name.S')
      .noUnmarshall()
      .contentOnly()
      .execute()
      .parse();
  assert.deepEqual(content, ['Comedy']);
});

test('no-unmarshall with pretransform -> pretransform on raw structure', async () => {
  const { content } = await new ReadCommand()
      .partitionKey('.entity = "category"')
      .pretransform('.name.S')
      .where('. == "Comedy"')
      .noUnmarshall()
      .execute()
      .parse();
  assert.deepEqual(content, ['Comedy']);
});

test('no-unmarshall with aggregate -> aggregation on raw items', async () => {
  const { content } = await new ReadCommand()
      .partitionKey('.entity = "language"')
      .transform('.name.S')
      .aggregate('map(gsub("^ +| +$"; "")) | sort')
      .noUnmarshall()
      .execute()
      .parse();
  assert.deepEqual(
      content,
      ['English', 'French', 'German', 'Italian', 'Japanese', 'Mandarin']
  );
});

test('no-unmarshall with list type -> L type descriptor', async () => {
  const content = await new ReadCommand()
      .key('.entity = "film" | .id = 50')
      .select('special_features')
      .noUnmarshall()
      .contentOnly()
      .execute()
      .parse();
  assert.deepEqual(
      content,
      [
        {
          special_features: {
            L: [
              { S: 'Commentaries' },
              { S: 'Behind the Scenes' }
            ]
          }
        }
      ]
  );
});

test('no-unmarshall lastEvaluatedKey remains unmarshalled', async () => {
  const { meta } = await new ReadCommand()
      .scanLimit(1)
      .noUnmarshall()
      .execute()
      .parse();
  assert.property(meta, 'lastEvaluatedKey');
  assert.strictEqual(typeof meta.lastEvaluatedKey.id, 'number');
  assert.strictEqual(typeof meta.lastEvaluatedKey.entity, 'string');
});
