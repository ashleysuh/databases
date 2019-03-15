#include <cassert>
#include <algorithm>
#include "QueryProcessor.h"
#include "Table.h"
#include "Index.h"
#include "Iterator.h"
#include "Row.h"
#include "ColumnSelector.h"
#include "RowCompare.h"
#include "Operators.h"
#include "util.h"

//----------------------------------------------------------------------

// TableIterator 

unsigned TableIterator::n_columns() 
{
    return _table->columns().size();
}

void TableIterator::open() 
{
   _input = _table->rows().begin();
}

Row* TableIterator::next() 
{   
    Row* row = NULL;

    if( _input != _end ){
        row = *_input;
        _input++;
    }
    
    return row;
}

void TableIterator::close() 
{
    _input = _table->rows().end();
    _end = _table->rows().end();
}

TableIterator::TableIterator(Table* table)
    : _table(table)
{
    _input = _table->rows().begin();
    _end = _table->rows().end();
}

//----------------------------------------------------------------------

// IndexScan

unsigned IndexScan::n_columns()
{
    return _index->n_columns();
}

void IndexScan::open()
{
    _input = _index->begin();
    _end = _index->end();
}

Row* IndexScan::next()
{
    Row* next = NULL;

    if( !_index->empty() ){
      while( _input != _end ){
        if( _input->first.at(0) >= _lo->at(0) && _input->first.at(0) <= _hi->at(0) ){
          next = _input->second;
          _input++;
          return next;
        }
        else{
          _input++;
        }
      }
    }

    return next;
}

void IndexScan::close()
{
    _input = _index->end();
}

IndexScan::IndexScan(Index* index, Row* lo, Row* hi)
    : _index(index),
      _lo(lo),
      _hi(hi == NULL ? lo : hi)
{}

//----------------------------------------------------------------------

// Select

unsigned Select::n_columns()
{
    return _input->n_columns();
}

void Select::open()
{
    _input->open();
}

Row* Select::next()
{
    Row* selected = NULL;
    Row* row = _input->next();
    
    while( row != NULL ){
        if( _predicate(row) ){
            selected = row;
            return selected;
        }

        row = _input->next();
    }
    return selected;
}

void Select::close()
{ 
    _input->close();
}

Select::Select(Iterator* input, RowPredicate predicate)
    : _input(input),
      _predicate(predicate)
{
}

Select::~Select()
{
    delete _input;
}

//----------------------------------------------------------------------

// Project

unsigned Project::n_columns()
{
    return _column_selector.n_selected();
}

void Project::open()
{
    _input->open();
}

Row* Project::next()
{
    Row* projected = NULL;
    Row* row = _input->next();
    if (row) {
        projected = new Row();
        for (unsigned i = 0; i < _column_selector.n_selected(); i++) {
            projected->append(row->at(_column_selector.selected(i)));
        }
        Row::reclaim(row);
    }
    return projected;
}

void Project::close()
{
    _input->close();
}

Project::Project(Iterator* input, const initializer_list<unsigned>& columns)
    : _input(input),
      _column_selector(input->n_columns(), columns)
{}

Project::~Project()
{
    delete _input;
}

//----------------------------------------------------------------------

// NestedLoopsJoin

unsigned NestedLoopsJoin::n_columns()
{
  if( _left_join_columns.n_selected() >= 0 && _right_join_columns.n_selected() >= 0 ){
    return _left_join_columns.n_columns() + _right_join_columns.n_columns() - 1;
  }

  return 0;
}

void NestedLoopsJoin::open()
{
  _left->open();
  _right->open();

  _left_row = _left->next();
}

bool NestedLoopsJoin::match(const Row* left, const Row* right)
{
  return (left->at(_left_join_columns.selected(0)) == right->at(_right_join_columns.selected(0)));
}

Row* NestedLoopsJoin::join_rows_if_match(const Row* left, const Row* right)
{
  Row* join_row = new Row();

  for( unsigned i = 0; i < left->size(); i++ ){
    join_row->append(left->at(i));
  }

  for( unsigned i = 0; i < right->size(); i++ ){
    if( i != _right_join_columns.selected(0) ){
      join_row->append(right->at(i));
    }
  }

  return join_row;
}

Row* NestedLoopsJoin::next()
{

  Row* nested = NULL;
  Row* r_row = _right->next();

  while( _left_row != NULL && r_row != NULL && !match(_left_row, r_row) ){
    _left_row = _left->next();

    if( _left_row == NULL ){
      r_row = _right->next();

      if( r_row != NULL ){
        _left->close();
        _left->open();
        _left_row = _left->next();
      }
    }

  }

  if( _left_row != NULL && r_row != NULL ){
    nested = join_rows_if_match(_left_row, r_row);
    Row::reclaim(r_row);
    //Row::reclaim(_left_row);
  }
  
  return nested;

  // return
  //   _left_row != NULL && r_row != NULL
  //   ? (join_rows_if_match(_left_row, r_row); Row::reclaim(r_row))
  //   : NULL;
}

void NestedLoopsJoin::close()
{
    _left->close();
    _right->close();
}

NestedLoopsJoin::NestedLoopsJoin(Iterator* left,
                                 const initializer_list<unsigned>& left_join_columns,
                                 Iterator* right,
                                 const initializer_list<unsigned>& right_join_columns)
    : _left(left),
      _right(right),
      _left_join_columns(left->n_columns(), left_join_columns),
      _right_join_columns(right->n_columns(), right_join_columns),
      _left_row(NULL)
{
    assert(_left_join_columns.n_selected() == _right_join_columns.n_selected());
}

NestedLoopsJoin::~NestedLoopsJoin()
{
    delete _left;
    delete _right;
    Row::reclaim(_left_row);
}

//----------------------------------------------------------------------

// Sort

unsigned Sort::n_columns() 
{
    return _input->n_columns();
}

void Sort::open() 
{
    _input->open();

    Row* row = _input->next();
    _sorted.clear();

    while( row != NULL ){
      _sorted.emplace_back(row);
      row = _input->next();
    }

    std::sort(_sorted.begin(), _sorted.end(), RowCompare(_sort_columns));
    _sorted_iterator = _sorted.begin();

    Row::reclaim(row);
}


Row* Sort::next() 
{
    Row* next = NULL;

    if( _sorted_iterator != _sorted.end() ){
      next = *_sorted_iterator;
      _sorted_iterator++;
    }

    return next;
}

void Sort::close() 
{
    _input->close();
}

Sort::Sort(Iterator* input, const initializer_list<unsigned>& sort_columns)
    : _input(input),
      _sort_columns(sort_columns)
{}

Sort::~Sort()
{
    delete _input;
    delete *_sorted_iterator;
}

//----------------------------------------------------------------------

// Unique

unsigned Unique::n_columns()
{
    return _input->n_columns();
}

void Unique::open() 
{
    _input->open();
}

Row* Unique::next()
{
  Row* row = _input->next();

  while( row != NULL ){
    // this must be the first iteration so set our _last_unique to first row
    if( _last_unique == NULL ){
      _last_unique = row;
      return _last_unique;
    }
    else{
      // only run this test if the current row's size is equal to last row
      if( row->size() == _last_unique->size() ){
        bool equal = true;

        for( unsigned i = 0; i < row->size(); i++ ){
          if( row->at(i).compare(_last_unique->at(i)) != 0 ){
            equal = false;
          }
        }
        // our rows weren't equal, so set _last_unique to current row
        if( !equal ){
          _last_unique = row;
          return _last_unique;
        }
      }
    }

    row = _input->next();
  }

  return NULL;
}

void Unique::close() 
{
    _input->close();
}

Unique::Unique(Iterator* input)
    : _input(input),
      _last_unique(NULL)
{}

Unique::~Unique()
{
    delete _input;
    Row::reclaim(_last_unique);
}
