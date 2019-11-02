import { Component, OnInit } from '@angular/core';
import { FormControl } from '@angular/forms';
import { Observable } from 'rxjs';
import { SearchService, Query } from '../search.service';
import { ActivatedRoute, Router } from '@angular/router';
import {
  debounceTime, filter
} from 'rxjs/operators';
import { DialogComponent } from './dialog/dialog.component';
import { MatDialog } from '@angular/material/dialog';

@Component({
  selector: 'app-catalog',
  templateUrl: './catalog.component.html',
  styleUrls: ['./catalog.component.css']
})
export class CatalogComponent implements OnInit {
  API_URL = '/api/';
  categories = [];
  styles = [];
  breweries: Observable<any>;
  sortByField = 'name';
  sortByDirection = 'Ascending';
  categoryField: FormControl;
  styleField: FormControl;
  breweryField: FormControl;
  abvField: FormControl;
  ibuField: FormControl;
  labelField: FormControl;
  searchField: FormControl;
  results: Observable<any>;
  lat = 34.0030;
  lng = -118.4298;

  constructor(private searchService: SearchService, private route: ActivatedRoute, private router: Router, public dialog: MatDialog) { }

  openDescriptionDialog(product: any) {
    this.dialog.open(DialogComponent, {
      data: product
    });
  }

  ngOnInit() {
    this.categoryField = new FormControl();
    this.styleField = new FormControl();
    this.breweryField = new FormControl();
    this.abvField = new FormControl();
    this.ibuField = new FormControl();
    this.labelField = new FormControl();
    this.labelField.setValue('all');
    this.searchField = new FormControl();
    this.searchField.setValue('');
    this.categoryField.valueChanges.subscribe(
      (category: string) => this.searchService.styles(category).subscribe(
        data => this.styles = data
      )
    );
    this.categoryField.valueChanges.subscribe(
      (category: string) => this.addQueryCriteria('@category:{' + category + '}')
    );
    this.styleField.valueChanges.subscribe(
      (style: string) => this.addQueryCriteria('@style:{' + style + '}')
    );
    this.breweryField.valueChanges.pipe(
      debounceTime(300)
    ).subscribe(prefix => this.searchService.suggestBreweries(prefix).subscribe(data => this.breweries = data));
    this.abvField.valueChanges.subscribe(
      (abv: string) => this.addQueryCriteria('@abv:[' + abv.replace('-', ' ') + ']')
    );
    this.ibuField.valueChanges.subscribe(
      (ibu: string) => this.addQueryCriteria('@ibu:[' + ibu.replace('-', ' ') + ']')
    );
    this.labelField.valueChanges.pipe(filter((label: string) => label === 'required')).subscribe(
      (label: string) => this.addQueryCriteria('@label:{true}')
    );
    this.searchService.categories().subscribe(data => this.categories = data);
    if (navigator.geolocation) {
      navigator.geolocation.getCurrentPosition((position) => {
        this.lat = position.coords.latitude;
        this.lng = position.coords.longitude;
      });
    } else {
      alert('Geolocation is not supported by this browser.');
    }
  }

  addQueryCriteria(criteria: string) {
    const query = this.searchField.value ? this.searchField.value + ' ' : '';
    this.searchField.setValue(query + criteria);
  }

  search() {
    const queryObject: Query = { query: this.searchField.value, sortByField: this.sortByField, sortByDirection: this.sortByDirection };
    this.results = this.searchService.products(queryObject, this.lng, this.lat);
  }

  displayBrewery(brewery: any) {
    if (brewery) { return brewery.name; }
  }

  brewerySelected(brewery: any) {
    this.addQueryCriteria('@brewery:{' + brewery.id + '}');
  }

}
