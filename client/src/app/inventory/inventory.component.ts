import { Component, OnInit, ViewChild } from '@angular/core';
import { ActivatedRoute } from "@angular/router";
import { StompService, StompConfig } from '@stomp/ng2-stompjs';
import { HttpClient } from '@angular/common/http';
import { MatTable } from '@angular/material';
import { SearchService } from '../search.service';

export interface InventoryData {
  store: string;
  sku: string;
  description: string;
  id: string;
  name: string;
  quantity: number;
  time: string;
}

@Component({
  selector: 'app-inventory',
  templateUrl: './inventory.component.html',
  styleUrls: ['./inventory.component.css']
})
export class InventoryComponent implements OnInit {

  API_URL = '/api/';
  @ViewChild(MatTable, { static: true }) table: MatTable<any>;

  private stompService: StompService;
  inventory: InventoryData[];
  store: string;
  displayedColumns: string[] = ['store', 'label', 'name', 'quantity'];

  constructor(private http: HttpClient, private route: ActivatedRoute, private searchService: SearchService) { }

  ngOnInit() {
    this.store = this.route.snapshot.queryParamMap.get("store");
    console.log(this.store);
    this.searchService.inventory(this.store).subscribe((inventory: InventoryData[]) => this.inventory = inventory);
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
    this.inventory = this.inventory.filter((value, key) => {
      if (value.id == row_obj.id) {
        if ('quantity' in row_obj) {
          value.quantity = row_obj.quantity;
        }
        if ('time' in row_obj) {
          value.time = row_obj.time;
        }
      }
      return true;
    });
    this.table.renderRows();
  }

  isRecent(time: string) {
    var updateTime = new Date(time);
    var currentTime = new Date();
    var duration = currentTime.valueOf() - updateTime.valueOf();
    return duration<1000;
  }

}
