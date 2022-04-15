import * as _ from 'lodash';

export const EMPTY_GRAPH:IGraph = {
  datasource: undefined,
  labels: { nodes: [], edges: [] },
  nodes: [],
  edges: []
};

export interface IGraph {
  datasource: string;
  labels: ILabels;
  nodes: IElement[];
  edges: IElement[];
};

export interface ILabels {
  meta?: any;                     // { V:{ labelName: totalSize, ... }, E:{ ... }}
  nodes: ILabel[];                // { idx, name, size, total }
  edges: ILabel[];
};

export interface ILabel {
  idx: number,                    // desc by size
  name: string;
  size: number;
  total: number;                  // from meta
  elements?: IElement[];          // e.data.label = ${name}
  color?: string;                 // node: background-color, edge: target-arrow-color
}

export interface IElement {
  group: string;                  // group = {'nodes', 'edges'}
  data: {
    datasource: string;
    id: string;
    label: string;
    properties: any;              // default={}
    name?: string;                // for only VERTEX
    source?: string;              // for only EDGE
    target?: string;              // for only EDGE
  };
  position?: {                    // for only NODE
    x: undefined,
    y: undefined
  }
  scratch: {
    _idx?: number                 // for only NODE
    _color?: any;                 // node: color, edge: [sourceColor, targetColor]
    _label?: ILabel;
    _source?: IElement;           // for only EDGE
    _target?: IElement;           // for only EDGE
  };
};

///////////////////////////////////////////////////////////////

export class Element implements IElement {
  readonly group: string;         // group == 'nodes'
  data: {
    datasource: string;
    id: string;
    label: string;
    properties: Map<string,any>;  // default={}
  };
  scratch: {
  };

  private classes: string[] = [];
  private style: any = {}

  constructor(ele:IElement){
    this.group = ele.group;
    this.data = ele.data;
    this.scratch = ele.scratch;
    // generate random id
    if( this.data.id === undefined ){
      let chars = "abcdefghijklmnopqrstufwxyzABCDEFGHIJKLMNOPQRSTUFWXYZ1234567890";
      this.data.id = this.data.datasource+'_'+this.data.label+'_'
                    +_.sampleSize(chars, 10).join('');    // ds + label + char(10)
    }
  }

  get type():string { return this.group; };
  get id():string { return this.data.id; }
  set id(id:string) { this.data.id = id; }
  get label():string { return this.data.label; };
  set label(name:string) { this.data.label = name; };

  keys(){ return this.data.properties.keys(); }
  values(){ return this.data.properties.values(); }

  properties(...keys:string[]){
    var values = new Array<any>();
    for( let key of keys ){
      if(this.data.properties.has(key))
        values.push(this.data.properties.get(key));
    }
    return values;
  }

  property(key:string, value:any){
    this.data.properties.set(key, value);
  }

  getClasses(){ return this.classes.join(','); }
  addClass(clss:string) {
    if( !this.classes.includes(clss) ) this.classes.push(clss);
  }
  removeClass(clss:string) {
    const index = this.classes.indexOf(clss, 0);
    if (index > -1) {
        this.classes.splice(index, 1);
    }
  }

  getStyle(){ return this.style; }
  setStyle(style:any){ this.style = style; }

  toElement(){
    return <IElement>{
      group: this.group,
      data: _.cloneDeep(this.data),
      scratch: _.cloneDeep(this.data)
    };
  }
};

export class Vertex extends Element {
  private readonly cy:any;

  constructor(cy:any, ele:IElement){
    super(ele);
    this.cy = cy;
  }

  // getNeighbors(direction:Direction){
  // }
};

export class Edge extends Element {
  private readonly cy:any;

  constructor(cy:any, ele:IElement){
    super(ele);
    this.cy = cy;
  }
};

///////////////////////////////////////////////////////////////

export enum Direction {
  IN = 1,
  OUT,
  BOTH
};

export interface IStyle {             // <== element.scratch('_style')
  color: string;                      // # + HEX
  width: number;                      // NODE: width, height | EDGE: width
  title: string;                      // one of keys of props (default: 'name')
  visible: boolean;                   // 'visible' = true, 'hidden' = false
  image?: string;                     // background-image (only for node)
  opacity?: number;                   // 0.0 ~ 1.0
};
