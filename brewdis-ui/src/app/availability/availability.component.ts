import { Component, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { SearchService } from '../search.service';

@Component({
  selector: 'app-availability',
  templateUrl: './availability.component.html',
  styleUrls: ['./availability.component.css']
})
export class AvailabilityComponent implements OnInit {

  title = 'Product Availability';
  lat = 34.0030;
  lng = -118.4298;
  stores: [];

  constructor(private searchService: SearchService, private route: ActivatedRoute) { }

  ngOnInit() {
    let sku: string;
    this.route.paramMap.subscribe(params => {
      sku = params.get('sku');
    });
    if (navigator.geolocation) {
      navigator.geolocation.getCurrentPosition((position) => {
        this.lat = position.coords.latitude;
        this.lng = position.coords.longitude;
        this.searchService.availability(sku, this.lng, this.lat).subscribe((data: []) => this.stores = data);
      });
    } else {
      alert('Geolocation is not supported by this browser.');
    }
  }

}
