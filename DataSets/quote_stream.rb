#!/usr/bin/ruby

class QuoteStream
  QUOTE_DIR = 'n100'
  def initialize sym
    filename = "#{QUOTE_DIR}/table_#{sym.downcase}.csv"
    @f = File.open(filename)
    @unread = nil
    raise "can't open #{filename} for QuoteStream" unless @f
  end

  def read
    if @unread
      tmp = @unread
      @unread = nil
      return tmp
    end
    return nil unless @f
    while line = @f.readline do
      row = line.strip.split(',')
      ttime = row[1].to_i
      return row if 930 <= ttime && ttime <= 1600
    end
    @f.close
    @f = nil
    nil
  rescue EOFError
    @f.close
    @f = nil
    nil
  end
  def unread row
    @unread = row
  end
end
