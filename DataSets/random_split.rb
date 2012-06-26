#!/usr/bin/ruby

shard_count = 5
shards = []
shard_count.times do |i|
  shards.push File.open("qqq_state_#{i}.csv","w")
end

line_count = 0
File.open("qqq_state.csv") do |f|
  begin
    header = f.readline
    shards.each { |s| s.write header }
    while line = f.readline
      shards[rand(shard_count)].write line
      line_count += 1
      if line_count % 10000 == 0
        puts "lines = #{line_count}"
      end
    end
  rescue EOFError => e
  end
end

shards.each { |s| s.close }
