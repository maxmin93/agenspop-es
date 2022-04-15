import { TestBed } from '@angular/core/testing';

import { DataSrcService } from './data-src.service';

describe('DataSrcService', () => {
  beforeEach(() => TestBed.configureTestingModule({}));

  it('should be created', () => {
    const service: DataSrcService = TestBed.get(DataSrcService);
    expect(service).toBeTruthy();
  });
});
