import { Component, OnInit, Input } from '@angular/core';
import { NgbActiveModal, NgbModal } from '@ng-bootstrap/ng-bootstrap';
import { ILabel, IElement } from 'src/app/models/agens-graph-types';
import * as _ from 'lodash';

const SELECT_NONE:string = 'select..';

@Component({
  selector: 'app-gcmodal',
  templateUrl: './gcmodal.component.html',
  styleUrls: ['./gcmodal.component.css']
})
export class GcModalComponent implements OnInit {

  @Input() public labels:ILabel[];
  @Input() public option:any;
  @Input() public cy:any;

  methodSelected: string = 'centrality';  // or label-value

  centralities:string[] = ['degree', 'pagerank', 'closeness', 'betweenness'];
  centralitySelected: string = SELECT_NONE;

  labelSelected: any = { name: SELECT_NONE };
  keys:any[] = [];
  keySelected: any = SELECT_NONE;

  constructor(public modal: NgbActiveModal) { }

  ngOnInit() {
    if( this.option ){
      this.methodSelected = this.option.method;
      if( this.methodSelected == 'centrality' ) this.centralitySelected = this.option.data;
      else{
        this.labelSelected = { name: this.option.data.label };
        this.keySelected = this.option.data.key;
      }
    }
  }

  onChangeMethod(value){
    this.methodSelected = value;
    if( value == 'centrality' ){
      this.labelSelected = { name: SELECT_NONE };
      this.keys = [];
      this.keySelected = 'SELECT_NONE';
    }
    else if( value == 'label-value' ){
      this.centralitySelected = SELECT_NONE;
    }
  }

  onChangeFunction(item){
    this.centralitySelected = item;
  }

  onChangeLabel(item:ILabel){
    this.labelSelected = item;

    let keys = new Set<string>();
    for( let e of item.elements ){
      keys = new Set([...keys, ...Object.keys(e.data.properties)]);   // merge set with array
    }
    this.keys = [...keys];
    this.keySelected = '_all_';
  }

  onChangeKey(item){
    this.keySelected = item;
  }

  close(state:boolean){
    let result = {
      method: state ? this.methodSelected : 'stop',
      data: this.methodSelected == 'centrality' ? this.centralitySelected : {
        label: this.labelSelected.name, key: this.keySelected == '_all_' ? null : this.keySelected
      }
    };
    this.modal.close(result);
  }

}
