import { Command } from "./Command";

export class ReadCommand extends Command {

  readonly aggregate = this.arg<string>('aggregate');
  readonly concurrency = this.arg<number>('concurrency');
  readonly consistentRead = this.flag('consistent-read');
  readonly compact = this.flag('compact');
  readonly endpointUrl = this.arg<string>('endpoint-url');
  readonly from = this.arg<string>('from');
  readonly rearrangeAttrs = this.flag('rearrange-attrs');
  readonly globalIndex = this.arg<string>('global-index');
  readonly startKey = this.arg<Record<string, string | number>>('start-key');
  readonly limit = this.arg<number>('limit');
  readonly scanLimit = this.arg<number>('scan-limit');
  readonly partitionKey = this.arg<string>('partition-key');
  readonly select = this.arg<string>('select');
  readonly sortKey = this.arg<string>('sort-key');
  readonly pretransform = this.arg<string>('pretransform');
  readonly transform = this.arg<string>('transform');
  readonly where = this.arg<string>('where');

  constructor() {
    super();
    this.endpointUrl('http://localhost:8000');
    this.from('dvd_rental');
  }

}