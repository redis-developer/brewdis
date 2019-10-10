import { Component, OnInit, ViewChild } from '@angular/core';
import { StompService, StompConfig } from '@stomp/ng2-stompjs';
import { HttpClient } from '@angular/common/http';
import { MatTable } from '@angular/material';

export interface InventoryData {
  store: string;
  sku: string;
  id: string;
  name: string;
  quantity: number;
  delta: number;
  adjust: string;
}

@Component({
  selector: 'app-inventory',
  templateUrl: './inventory.component.html',
  styleUrls: ['./inventory.component.css']
})
export class InventoryComponent implements OnInit {

  API_URL = '/api/';
  @ViewChild(MatTable,{static:true}) table: MatTable<any>;

  private stompService: StompService;
  inventory: InventoryData[];
  displayedColumns: string[] = ['id','label','name','quantity'];

  constructor(private http: HttpClient) { }

  ngOnInit() {
    this.http.get(this.API_URL + 'inventory').subscribe((inventory: InventoryData[]) => this.inventory = inventory);
    this.http.get(this.API_URL + 'config/stomp').subscribe((stomp: any) => this.connectStompService(stomp));
  }

  connectStompService(config: any) {
    const stompUrl = config.protocol + '://' + config.host + ':' + config.port + config.endpoint;
    const stompConfig: StompConfig = {
      url: stompUrl,
      headers: {
        login: '',
        passcode: ''
      },
      heartbeat_in: 0,
      heartbeat_out: 20000,
      reconnect_delay: 5000,
      debug: true
    };
    this.stompService = new StompService(stompConfig);
    this.stompService.subscribe(config.inventoryTopic).subscribe(update => this.updateRowData(JSON.parse(update.body)));
  }
  
  updateRowData(row_obj) {
    this.inventory = this.inventory.filter((value,key)=>{
      if(value.id == row_obj.id){
    	if ('quantity' in row_obj) {
    		value.quantity = row_obj.quantity;
    	}
    	if ('delta' in row_obj) {
    		value.delta = row_obj.delta;
    	}
    	if ('adjust' in row_obj) {
    		value.adjust = row_obj.adjust;
    	} else {
    		row_obj.adjust = 'false';
    	}
      }
      return true;
    });
    this.table.renderRows();
  }

}
