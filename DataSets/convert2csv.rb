#!/usr/bin/ruby
require 'quote_stream'

# assume nasdaq_100 and qqq are unpacked here
RAW_DIR = 'n100'

files = Dir["#{RAW_DIR}/table*.csv"].map
files.sort!
qqq_file = "#{RAW_DIR}/table_qqq.csv"
files.delete qqq_file
files.unshift qqq_file
#files = files[0..10]

def prettyBytes bytes
  if bytes < 1000
    bytes
  elsif bytes < 1000*1000
    "#{bytes/1000}k"
  else
    "#{bytes/1000/1000}m"
  end
end

syms = []
streams = files.map do |filename|
  if /_([a-z]+)/ =~ filename
    sym = $1.upcase
    #puts sym
    syms.push sym
    [sym, QuoteStream.new(sym)]
  else
    raise "what?: #{filename}"
  end
end

expected_cols = streams.size * 5 + 2

qqq_stream = streams.shift

f = File.open("qqq_state.csv","w")

f.write "date,time"
hl = ['_open','_high','_low','_close','_volume']
syms.each do |sym|
  f.write ',' + (hl.map { |s| sym.downcase + s}.join(','))
end
f.write "\n"
last_row = nil

count = 0
bytes = 0

while qqq = qqq_stream[1].read do
  if count % 1000 == 0
    puts "row #{count}   bytes = #{prettyBytes bytes}"
  end
  puts "initial row complete" if count == 1
  count += 1
  qqq[1] = qqq[1].to_i
  out = []
  out.push qqq[0..6]
  #puts "#{qqq_stream[0]}: #{qqq.join(',')}"

  streams.each do |sym, stream|
    while row = stream.read
      if row[0].class == Float
        puts row.inspect
      end
      if qqq[0] < row[0] ||
          qqq[0] == row[0] && qqq[1] < row[1].to_i 
        # row is in the future
        stream.unread row
        row[2] = 0.0
        row[3] = 0.0
        row[4] = 0.0
        row[5] = 0.0
        row[6] = 0.0
        #puts "#{sym}: #{row.join(',')}"
        out.push row[2..6]
        break
      elsif qqq[0] == row[0] && qqq[1] == row[1].to_i
        # row is in the future
        #puts "#{sym}: #{row.join(',')}"
        out.push row[2..6]
        break
        #else row is in the past, read the next
      end
    end
  end
  x = out.join(',')
  xs = x.split(',')
  xsf = xs.map { |v| v.to_f }
  xsf[0] = xs[0]
  xsf[1] = xs[1].to_i
  if last_row
  #raise "not enough columns #{xs.size} != #{expected_cols}\n#{x}" unless xs.size == expected_cols
    old = last_row
    last_row = xsf.dup
    (2..(old.size-1)).each do |i|
      begin
        if old[i] > 0.01
          xsf[i] = (10000.0*(xsf[i] - old[i])/ old[i] + 0.5 ).to_i/100.0
        else
          xsf[i] = 0
        end
      rescue Exception => e
        puts e.message
        puts "index = #{i}"
        puts "last"
        puts old.inspect
        puts "cur"
        puts last_row.inspect
        xsf[i] = 0
      end
    end
    outx = xsf.join(',')
    f.puts outx
    bytes += outx.size + 1
  else
    last_row = xsf
  end
  
end
