import { Command }    from './Command.js';
import { TABLE_NAME } from '../table.js';

export class ReadCommand extends Command {

  readonly aggregate = this.arg<string>('aggregate');
  readonly concurrency = this.arg<number>('concurrency');
  readonly contentOnly = this.flag('content-only');
  readonly consistentRead = this.flag('consistent-read');
  readonly endpointUrl = this.arg<string>('endpoint-url');
  readonly from = this.arg<string>('from');
  readonly reduce = this.arg<string[]>('reduce');
  readonly rearrangeKeys = this.flag('rearrange-keys');
  readonly index = this.arg<string>('index');
  readonly startKey = this.arg<string | object>('start-key');
  readonly limit = this.arg<number>('limit');
  readonly scanLimit = this.arg<number>('scan-limit');
  readonly partitionKey = this.arg<string | object>('partition-key');
  readonly select = this.arg<string>('select');
  readonly sortKey = this.arg<string | object>('sort-key');
  readonly stream = this.flag('stream');
  readonly pretransform = this.arg<string>('pretransform');
  readonly transform = this.arg<string>('transform');
  readonly where = this.arg<string>('where');
  readonly expand = this.flag('expand');
  readonly requestLimit = this.arg<number>('request-limit');
  readonly itemsPerRequest = this.arg<number>('items-per-request');
  readonly metadataOnly = this.flag('meta-only');
  readonly prune = this.arg<string>('prune');

  constructor() {
    super();
    this.endpointUrl(process.env.DYNAMODB_ENDPOINT_URL ?? 'http://localhost:8000');
    this.from(TABLE_NAME);
    this.quiet();
  }

}