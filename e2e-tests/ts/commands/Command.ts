import { exec }           from 'child_process';
import { AssertionError } from 'chai';

export abstract class Command {

  private readonly options = new Map<string, unknown>();

  execute(): CommandOutput {
    const prom = new Promise<string>((resolve, reject) => {
      exec(`./dynq ${this.buildArgs()}`, (err, stdout) => {
        if (err) {
          reject(err);
        }
        resolve(stdout);
      });
    });
    return {
      raw: () => prom,
      parse: () => prom.then(stdout => {
        try {
          return JSON.parse(stdout);
        } catch (err) {
          throw new AssertionError('expected JSON string, received ' + stdout);
        }
      })
    };
  }

  protected arg<T>(name: string) {
    return (value: T) => {
      this.options.set(name, value);
      return this;
    };
  }

  protected flag(name: string) {
    return () => {
      this.options.set(name, true);
      return this;
    };
  }

  private buildArgs(): string {
    return [...this.options.entries()].flatMap(([name, value]) => {
      const output = [`--${name}`];
      if (value !== true) {
        output.push(`'${typeof value === 'object' ? JSON.stringify(value) : value}'`);
      }
      return output;
    }).join(' ');
  }

}

export interface CommandOutput {
  raw(): Promise<string>;
  parse(): Promise<any>;
}