import {Component, OnInit, Input, Output, EventEmitter} from '@angular/core';
import { IElement } from 'src/app/models/agens-graph-types';
// import {NodeInfo} from "../../models/data-model";
import {PALETTE_DARK, PALETTE_ICON} from 'src/app/utils/palette-colors';
import { IEvent } from 'src/app/models/agens-data-types';

@Component({
  selector: 'app-property',
  templateUrl: './property.component.html',
  styleUrls: ['./property.component.css']
})
export class PropertyComponent implements OnInit {

  palette:string[] = PALETTE_DARK;
  icons:any[] = PALETTE_ICON;

  currIcon:any = undefined;
  currColor:string = '#DCDCDC';
  currColor1:string = '#DCDCDC';
  currColor2:string = '#0A0A0A';
  @Output() changeStyleEmitter= new EventEmitter<IEvent>();

  //public panelInfo: any = {}; // NodeInfo = new NodeInfo();
  togglePanel:boolean = false;

  isEdge:boolean = false;
  target:any = undefined;
  data:any = undefined;
  features:any = undefined;

  source_label:string = 'none'
  target_label:string = 'none'

  currMode:string;
  canPopover:boolean = false;
  @Input() set screenMode(mode:string) {
    this.currMode = mode;
    this.canPopover = (mode == 'canvas');
  }

  constructor() { }

  ngOnInit() {
  }

  /////////////////////////////////////////////////

	public showPanel(e:IElement) {
    this.target = e;
    // 색변경 팔레트 팝업하기 (캔버스에서만 작동)
    this.canPopover = (this.currMode == 'canvas' && e.group == 'nodes');

    this.data = e.data;
    this.features = Object.entries(e.scratch).map(([k,v]) => ({key: k, value: v}))
            .filter(x=> (<string>x.key).startsWith('_$$'));
            // .map(x=> x.key = x.key.substr(3));

    // edge 의 경우 '_color' 값이 array[2]로 온다
    if (Array.isArray(e.scratch['_color'])) {
      this.currColor1 = e.scratch.hasOwnProperty('_color') ? e.scratch['_color'][0] : '#DCDCDC';  // 회색
      this.currColor2 = e.scratch.hasOwnProperty('_color') ? e.scratch['_color'][1] : '#0A0A0A'; // 검정
      this.isEdge = true;
      console.log('showPanel:', e.scratch);
      if( e.scratch['_source'] ) this.source_label = e.scratch['_source']['data']['label'];
      if( e.scratch['_target'] ) this.target_label = e.scratch['_target']['data']['label'];
    }
    else{
      this.currColor = e.scratch.hasOwnProperty('_color') ? e.scratch['_color'] : '#DCDCDC';
      this.isEdge = false;
    }

    this.currIcon = e.scratch.hasOwnProperty('_icon') ? e.scratch['_icon'] : undefined;
    this.togglePanel = true;
  }

	public hidePanel() {
    this.togglePanel = false;
    this.data = undefined;
    this.features = undefined;
    // reset style
    this.currIcon = undefined;
    this.currColor = '#DCDCDC';
  }

  /////////////////////////////////////////////////

  selectColor(value){
    this.currColor = value;
    this.changeStyleEmitter.emit(<IEvent>{ type: 'color', data: {target: this.target, color: value} });
  }

  selectIcon(value){
    if( value.name == 'ban' ) this.currIcon = null;
    else this.currIcon = value;
    this.changeStyleEmitter.emit(<IEvent>{ type: 'icon', data: {target: this.target, icon: this.currIcon} });
  }
}