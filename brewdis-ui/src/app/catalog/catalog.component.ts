import { Component, OnInit } from '@angular/core';
import { FormControl } from '@angular/forms';
import { Observable } from 'rxjs';
import { SearchService, Query } from '../search.service';
import { ActivatedRoute, Router } from '@angular/router';
import {
  debounceTime, filter, share
} from 'rxjs/operators';
import { DialogComponent } from './dialog/dialog.component';
import { MatDialog } from '@angular/material/dialog';
import { PageEvent } from '@angular/material/paginator';

@Component({
  selector: 'app-catalog',
  templateUrl: './catalog.component.html',
  styleUrls: ['./catalog.component.css']
})
export class CatalogComponent implements OnInit {
  API_URL = '/api/';
  breweries: Observable<any>;
  foods: Observable<any>;
  sortByField = 'name';
  sortByDirection = 'ASC';
  showFilters = false;
  showSort = false;
  categoryField = new FormControl();
  categories = [];
  styleField = new FormControl();
  styles = [];
  breweryField = new FormControl();
  abvField = new FormControl();
  ibuField = new FormControl();
  foodField = new FormControl();
  labelField = new FormControl();
  searchField = new FormControl();
  showResults = false;
  result$: Observable<any> = null;
  resultCount = 0;
  searchDuration = 0;
  lat = 34.0030;
  lng = -118.4298;
  pageIndex = 0;
  pageSize = 50;

  constructor(private searchService: SearchService, private route: ActivatedRoute, private router: Router, public dialog: MatDialog) { }

  openDescriptionDialog(product: any) {
    this.dialog.open(DialogComponent, {
      data: product
    });
  }

  ngOnInit() {
    this.labelField.setValue('all');
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
    this.foodField.valueChanges.pipe(
      debounceTime(300)
    ).subscribe(prefix => this.searchService.suggestFoods(prefix).subscribe(data => this.foods = data));
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

  toggleFilters() {
    this.showFilters = !this.showFilters;
  }

  toggleSort() {
    this.showSort = !this.showSort;
  }

  addQueryCriteria(criteria: string) {
    const query = this.searchField.value ? this.searchField.value + ' ' : '';
    this.searchField.setValue(query + criteria);
  }

  search(pageIndex: number, pageSize: number) {
    const queryObject: Query = {
      query: this.searchField.value,
      sortByField: this.sortByField,
      sortByDirection: this.sortByDirection,
      pageIndex,
      pageSize
    };
    this.result$ = this.searchService.products(queryObject, this.lng, this.lat).pipe(share());
    this.result$.subscribe(r => {this.resultCount = r.count; this.searchDuration = r.duration; });
    this.showResults = true;
    this.pageIndex = pageIndex;
    this.pageSize = pageSize;
  }

  public handlePage(event: PageEvent) {
    this.pageIndex = event.pageIndex;
    this.pageSize = event.pageSize;
    this.search(event.pageIndex, event.pageSize);
  }

  displayBrewery(brewery: any) {
    if (brewery) { return brewery.name; }
  }

  displayFood(food: any) {
    if (food) {
      return food;
    }
  }

  brewerySelected(brewery: any) {
    this.addQueryCriteria('@brewery:{' + brewery.id + '}');
  }

  foodSelected(food: any) {
    this.addQueryCriteria('@foodPairings:("' + food + '")');
  }

}
