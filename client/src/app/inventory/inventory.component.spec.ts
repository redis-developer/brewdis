import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { FavoriteAlbumsComponent } from './favorite-albums.component';

describe('FavoriteAlbumsComponent', () => {
  let component: FavoriteAlbumsComponent;
  let fixture: ComponentFixture<FavoriteAlbumsComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ FavoriteAlbumsComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(FavoriteAlbumsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
