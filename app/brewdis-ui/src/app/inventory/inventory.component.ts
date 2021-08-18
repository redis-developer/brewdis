import { Component, OnInit, ViewChild } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { StompService, StompConfig } from '@stomp/ng2-stompjs';
import { HttpClient } from '@angular/common/http';
import { MatTable } from '@angular/material';
import { SearchService } from '../search.service';
import { MatSort } from '@angular/material/sort';
import { MatTableDataSource } from '@angular/material/table';

export interface InventoryData {
  store: string;
  sku: string;
  storeDescription: string;
  productName: string;
  availableToPromise: number;
  onHand: number;
  allocated: number;
  reserved: number;
  virtualHold: number;
  delta: number;
  time: string;
  level: string;
}

@Component({
  selector: 'app-inventory',
  templateUrl: './inventory.component.html',
  styleUrls: ['./inventory.component.css']
})
export class InventoryComponent implements OnInit {

  API_URL = '/api/';
  @ViewChild(MatTable, { static: true }) table: MatTable<any>;
  @ViewChild(MatSort, { static: true }) sort: MatSort;

  private stompService: StompService;
  dataSource = new MatTableDataSource();
  displayedColumns: string[] = ['store', 'sku', 'productName', 'availableToPromise', 'onHand', 'allocated', 'reserved', 'virtualHold'];

  constructor(private http: HttpClient, private route: ActivatedRoute, private searchService: SearchService) { }

  ngOnInit() {
    let store: string;
    this.route.paramMap.subscribe(params => {
      store = params.get('store');
    });
    this.dataSource.sort = this.sort;
    this.searchService.inventory(store).subscribe((inventory: InventoryData[]) => this.dataSource.data = inventory);
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
      debug: false
    };
    this.stompService = new StompService(stompConfig);
    this.stompService.subscribe(config.inventoryTopic).subscribe(update => this.updateRowData(JSON.parse(update.body)));
  }

  updateRowData(row) {
    this.dataSource.data = this.dataSource.data.filter((value: InventoryData, key) => {
      if (value.store === row.store && value.sku === row.sku) {
        value.availableToPromise = row.availableToPromise;
        value.onHand = row.onHand;
        value.allocated = row.allocated;
        value.reserved = row.reserved;
        value.virtualHold = row.virtualHold;
        value.time = row.time;
        value.delta = row.delta;
        value.level = row.level;
      }
      return true;
    });
    this.table.renderRows();
  }

  isRecent(row: InventoryData) {
    const duration = new Date().valueOf() - new Date(row.time).valueOf();
    return duration < 1000;
  }

}
