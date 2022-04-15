export enum DataType {
  NODE='nodes', EDGE='edges', GRAPH='graph', ID='id', NUMBER='number', STRING='string'
  , ARRAY='array', OBJECT='object', BOOLEAN='boolean', NULL='null'
};

export interface IRecord {
  group: string;                // group == 'record'
  columns: IColumn[];
  rows: IRow[];
};

export interface IColumn {
  group: string;                // group == 'columns'
  name: string;
  index: number;
  type: string;
};

export interface IRow {
  group: string;                // group == 'columns'
  index: number;
  row: any[];
};

///////////////////////////////////////////////////////////////

export interface IEvent {
  type: string;
  data: any;
}

export interface ISearch {
	ktype: string;
	vtype: string;
	value: any;
	key? : string;
}

